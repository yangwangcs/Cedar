# Cedar Patricia 32-by-8 Concurrency Stabilization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Permanently restore Cedar's pure Patricia 32-by-8 byte-segment representation, eliminate its eight-writer cross-seed instability without changing the representation or publication contract, and prove the retained design through freeze, Flush/SST, WAL recovery, and bidirectional format compatibility.

**Architecture:** Restore the already validated 32-by-8 code and tests first. Then evaluate two narrowly approved changes as separate commits: 128-byte physical spacing between segment publication edges and one compatible same-edge retry after a failed CAS. Keep only changes that pass correctness, Arena, and isolated Release A/B checks; after the five-seed final matrix passes, stop optimizing and validate the complete persistence chain with identical fixture source linked against old and final production revisions.

**Tech Stack:** C++20, embedded RocksDB 11.1.2, GoogleTest, CMake, Python 3 standard library, Bash, macOS Clang, Debug/Release/ASan/TSan builds.

**Spec:** `docs/superpowers/specs/2026-09-22-cedar-patricia-32x8-concurrency-stabilization-design.md`

## Global Constraints

- Work only in `/Users/wangyang/Desktop/Cedar/.worktrees/pure-radix-cas-cursor` on `codex/pure-radix-cas-cursor`; detached worktrees are allowed only for clean builds and old/new fixture binaries.
- Preserve exactly 40 normalized key bytes. Byte index 39 remains a valid Patricia discriminator.
- Retain exactly 32 byte segments of eight values each, eight-bit occupancy, and immutable exact-size child blocks.
- Published nodes and blocks remain immutable and Arena-owned. Root and segment publication remains CAS-only with release success and acquire failure/read ordering.
- Do not add locks, shards, staging indexes, fallback indexes, reclamation, background writers, or WAL/SST format fields.
- Do not edit, stage, or commit the user-owned dirty paths `benchmarks/cedar_radix_memtable_bench.cc`, `tests/performance/test_radix_benchmark_csv.cmake`, or `benchmarks/run_cedar_radix_read_matrix.sh`.
- Configure RocksDB with `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1` and build every named target with `-j1`.
- Timed builds use `/usr/bin/c++`, `Release`, and `-O3 -DNDEBUG`; timed runs do not enable diagnostic hooks or `--stats`.
- Binding gates for every seed: one-writer MemTable <=1.05x four-by-64 Byte; four-writer MemTable <=0.80x SkipList; eight-writer MemTable <=0.85x SkipList; candidate eight-writer CV <=5%; Arena <=1.25x Nibble at one, four, and eight writers.
- The final matrix uses seeds 20260920 through 20260924. It uses ten independent candidate and SkipList processes for eight writers, and five processes per compared implementation for one and four writers.
- Every timed row reports 131072 operations, zero errors, and the expected result hash. Per-seed medians are binding and are never averaged across seeds.
- Once every final performance gate passes, stop performance work and run the full lifecycle. Any correctness, sanitizer, persistence, or compatibility failure blocks finalization.

## Review Focus

- A 128-byte `SegmentEdge` must have 128-byte stride without requiring over-aligned allocation from RocksDB's pointer-aligned `Allocator`.
- A local retry may proceed only when the exact target child is unchanged; a changed child or second CAS failure must return to an acquire root load.
- A concurrent ancestor wrapper must keep a locally retried descendant reachable and ordered; no stale replacement may lose another writer's child.
- Segment spacing may reduce false sharing while increasing branch footprint and sparse scans; the one-writer and Arena gates must catch that trade-off.
- Format compatibility must be proved with old and final binaries reading each other's real SST and unflushed WAL directories, not inferred from source inspection.

---

### Task 1: Restore And Revalidate The 32-by-8 Baseline

**Files:**
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/models/cedar_pure_radix_model.py`
- Modify: `tests/models/cedar_pure_radix_interleaving_model.py`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`

**Interfaces:**
- Restores test commits `83ebaf0` and `77ab942` by reverting their revert commits `0ad965c` and `9f1178b`.
- Restores production commit `48bba50` by reverting its revert commit `763c141`.
- Produces `kByteSegmentCount == 32`, `SegmentForByte(value) == value >> 3`, `LocalByte(value) == value & 7`, and a `uint16_t` no-child sentinel of 256.

- [ ] **Step 1: Restore the representation tests and models before production.**

```bash
git revert --no-edit 0ad965c
git revert --no-edit 9f1178b
```

Expected: only the C++ representation tests and two Python models change; the three user-owned dirty paths remain unstaged.

- [ ] **Step 2: Run the models and compile to establish RED.**

```bash
python3 tests/models/cedar_pure_radix_model.py --check-negative-control
python3 tests/models/cedar_pure_radix_interleaving_model.py --check-negative-control
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
```

Expected: both models pass and detect their injected negative controls; the C++ build fails because the current four-by-64 production header does not satisfy the restored 32-by-8 contract.

- [ ] **Step 3: Restore the measured 32-by-8 production implementation.**

```bash
git revert --no-edit 763c141
```

Expected: only `cedar_pure_radix_index.h` and `.cc` change; the code matches commit `48bba50` for those files.

- [ ] **Step 4: Rebuild and prove GREEN.**

```bash
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R '^PartitionedVersionRadixMemTableTest\.' --output-on-failure
python3 tests/models/cedar_pure_radix_model.py --check-negative-control
python3 tests/models/cedar_pure_radix_interleaving_model.py --check-negative-control
```

Expected: all focused C++ tests pass, both models pass, and both negative controls are detected.

- [ ] **Step 5: Verify restoration identity and worktree scope.**

```bash
git diff --exit-code 48bba50 -- \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.h \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.cc
git diff --check
git status --short
```

Expected: production identity check exits 0; only the three pre-existing dirty paths remain outside committed restoration changes.

### Task 2: Isolate Segment Publication Edges At 128-byte Stride

**Files:**
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`

**Interfaces:**
- Add `static size_t SegmentEdgeStrideForTesting()` returning the actual `sizeof(SegmentEdge)`.
- `SegmentEdge` contains one naturally aligned `std::atomic<ChildBlock*> block` plus inert byte padding; `sizeof(SegmentEdge) == 128` and `alignof(SegmentEdge) <= alignof(std::max_align_t)`.
- Existing `BlockAt`, scans, statistics, and CAS publication access `segments[i].block`; no read or publication semantics change.

- [ ] **Step 1: Write a RED physical-layout test.** Add this focused contract:

```c++
TEST(PartitionedVersionRadixMemTableTest,
     SegmentPublicationEdgesUseIndependent128ByteStrides) {
  EXPECT_EQ(CedarPureRadixIndex::kByteSegmentCount, 32U);
  EXPECT_EQ(CedarPureRadixIndex::SegmentEdgeStrideForTesting(), 128U);
}
```

Extend `SnapshotDiagnosticsSeparateAllocationCopyAndPublication` so its branch-byte observer requires at least `32U * 128U` bytes and the existing insertion/order assertions still pass.

- [ ] **Step 2: Build the focused target to verify RED.**

```bash
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
```

Expected: compile failure naming `SegmentEdgeStrideForTesting`.

- [ ] **Step 3: Implement pointer-aligned 128-byte stride.** Use the following shape, deriving the padding from the atomic size:

```c++
static constexpr size_t kPublicationCacheLineBytes = 128;
struct SegmentEdge {
  std::atomic<ChildBlock*> block{nullptr};
  std::array<std::byte,
             kPublicationCacheLineBytes - sizeof(std::atomic<ChildBlock*>)>
      padding{};
};
static_assert(sizeof(SegmentEdge) == kPublicationCacheLineBytes);
static_assert(alignof(SegmentEdge) <= alignof(std::max_align_t));
```

Do not use `alignas(128)`: `Allocator::AllocateAligned` does not guarantee that alignment. Update all 32 segment loads, stores, and compare-exchanges through `.block` and report the new `sizeof(Branch)` through the existing allocation hook.

- [ ] **Step 4: Run focused correctness and layout verification.**

```bash
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R '^PartitionedVersionRadixMemTableTest\.' --output-on-failure
```

Expected: every focused test passes and the stride test reports 128.

- [ ] **Step 5: Commit the isolated change.**

```bash
git add src/engine/rocksdb/memtable/cedar_pure_radix_index.h \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.cc \
  src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc
git commit -m "perf: isolate radix segment publication edges"
```

### Task 3: Add One Compatible Same-edge Retry

**Files:**
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/models/cedar_pure_radix_interleaving_model.py`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`

**Interfaces:**
- Add disabled-by-default hooks `local_retry_for_testing(bool compatible, bool published)` and `root_restart_for_testing()`.
- Add private `enum class BlockUpdateKind { kInsert, kReplace }` and `PublishBlockUpdate(Branch*, uint8_t value, ChildBlock* observed, Node* expected_child, Node* desired_child, BlockUpdateKind)`.
- The helper performs the initial block allocation/CAS, then at most one compatible retry. It returns true only when one CAS publishes; false tells the caller to restart its outer acquire-root loop.

- [ ] **Step 1: Add RED deterministic interleavings.** Add these named tests using the existing `before_cas` condition-variable pattern:

```text
CompatibleEmptyChildFailureRetriesSameEdgeOnce
CompatibleWrapperFailureRetriesSameEdgeOnce
ChangedTargetChildFallsBackToAcquireRoot
SecondSameEdgeFailureFallsBackToAcquireRoot
CompatibleRetryRemainsReachableAfterConcurrentAncestorWrapper
```

Each test inserts unique 40-byte keys, joins every writer, verifies `Contains`, and compares a complete ordered cursor scan with the sorted expected entries. The changed-child test requires zero local retry attempts; the second-failure test requires exactly one local attempt and one root restart. The ancestor-wrapper test must exercise byte 39 as one differing position.

- [ ] **Step 2: Extend the interleaving model with the same state transitions.** Model `edge_snapshot`, `target_child`, one retry budget, and root-restart count. Its negative control deliberately retries after a changed child and must report the lost concurrent child.

- [ ] **Step 3: Run RED.**

```bash
python3 tests/models/cedar_pure_radix_interleaving_model.py --check-negative-control
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
```

Expected: the model contract passes and detects its negative control; C++ compilation fails on the new hook names or the new tests fail because all failed CAS operations still restart from root.

- [ ] **Step 4: Centralize segment publication and implement the bounded retry.** The compatibility check is exact:

```c++
Node* current_child = ChildAt(current_block, LocalByte(value));
const bool compatible =
    kind == BlockUpdateKind::kInsert ? current_child == nullptr
                                     : current_child == expected_child;
```

If compatible, allocate a fresh block from `current_block`, attempt exactly one more release/acquire CAS, and report it through the hooks. If incompatible or the second CAS fails, call the root-restart hook and return false. All three insertion sites call the helper and publish boundary updates only after true.

- [ ] **Step 5: Run GREEN and regression tests.**

```bash
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R '^PartitionedVersionRadixMemTableTest\.' --output-on-failure
python3 tests/models/cedar_pure_radix_model.py --check-negative-control
python3 tests/models/cedar_pure_radix_interleaving_model.py --check-negative-control
```

Expected: all focused tests and both models pass; both negative controls are detected.

- [ ] **Step 6: Commit the retry independently.**

```bash
git add src/engine/rocksdb/memtable/cedar_pure_radix_index.h \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.cc \
  src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc \
  tests/models/cedar_pure_radix_interleaving_model.py
git commit -m "perf: retry compatible radix segment publication"
```

### Task 4: Add The Five-seed Stability Matrix Contract

**Files:**
- Create: `benchmarks/run_cedar_patricia_32x8_stability_matrix.sh`
- Create: `benchmarks/summarize_cedar_patricia_32x8_stability.py`
- Create: `tests/performance/test_patricia_32x8_stability.cmake`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Runner: `--nibble ABS_BIN --byte ABS_BIN --baseline ABS_BIN --candidate ABS_BIN --output ABS_EMPTY_DIR`.
- Production seeds are `20260920 20260921 20260922 20260923 20260924`; repeats are 1-5 for one/four writers and 1-10 for eight writers.
- The runner writes unchanged 28-column raw benchmark CSV and a 29-column `variant,...` aggregate CSV. Candidate comparisons use Byte at one writer and SkipList from the Byte binary at four/eight writers; Nibble supplies Arena controls.
- Summarizer emits medians, population standard deviation, candidate CV, ratios, Arena ratios, sample counts, hashes, and `all_gates_pass`; exit 0 is pass and 11 is any malformed sample or failed binding gate.

- [ ] **Step 1: Register a RED CTest contract.** Add `Patricia32x8StabilityContract` beside the existing retention contract. In test mode the runner uses 64 entries, one seed, one repeat for one/four writers, and two repeats for eight writers. The CMake script requires 29 fields, zero errors, correct operations, no `radix_stats`, and runs the summarizer self-test.

- [ ] **Step 2: Run RED.**

```bash
cmake -S . -B /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON \
  -DCEDAR_ROCKSDB_CACHE_ROOT=/Volumes/E/CedarRocksDBCache \
  -DCEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R '^Patricia32x8StabilityContract$' --output-on-failure
```

Expected: failure because the new runner and summarizer do not exist.

- [ ] **Step 3: Implement strict runner validation.** Validate absolute executable paths, empty output, 28 fields, requested writer/phase, exact operation count, zero errors, and identical hash for every `(seed, phase, writers, repeat)` group. Interleave complete variant order using `(seed + writers + repeat) % 2`; never enable stats in timed processes.

- [ ] **Step 4: Implement evaluator and self-tests.** Compute CV as `statistics.pstdev(values) / statistics.mean(values)`. Self-tests must cover: exact pass at ratios 1.05/0.80/0.85 and CV 0.05; failure at eight-writer ratio 0.850001; failure at CV 0.050001; failure at Arena 1.250001; nonzero errors; hash mismatch; and missing tenth eight-writer sample.

- [ ] **Step 5: Verify GREEN and malformed-input rejection.**

```bash
bash -n benchmarks/run_cedar_patricia_32x8_stability_matrix.sh
python3 -m py_compile benchmarks/summarize_cedar_patricia_32x8_stability.py
python3 benchmarks/summarize_cedar_patricia_32x8_stability.py --self-test
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R '^Patricia32x8StabilityContract$' --output-on-failure
```

Expected: all commands pass.

- [ ] **Step 6: Commit tooling without touching user files.**

```bash
git add benchmarks/run_cedar_patricia_32x8_stability_matrix.sh \
  benchmarks/summarize_cedar_patricia_32x8_stability.py \
  tests/performance/test_patricia_32x8_stability.cmake tests/CMakeLists.txt
git commit -m "bench: verify radix eight-writer stability"
```

### Task 5: Measure Isolated Changes And Pass The Final Release Gates

**Files:**
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-stability/diagnostics.json`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-stability/matrix-raw/*.csv`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-stability/summary.json`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-stability/report.md`

**Interfaces:**
- Baseline revision is the Task 1 restored 32-by-8 commit; isolation revision is Task 2; final revision is Task 3.
- Existing controls are production revisions `77593a87a64f03d7716318e3c9ce7bb672a78327` (Nibble) and `76adf448257aaaa4eacac8ba5451ba38a51223f3` (four-by-64 Byte/SkipList).
- Each comparison binary comes from a clean detached worktree and a separate Release build directory under `/Volumes/E/CedarBuild`.

- [ ] **Step 1: Create clean builds and prove comparability.** For baseline, isolation, final, Nibble, and Byte revisions, record full commit, clean status, benchmark source SHA-256, compiler path, `CMAKE_BUILD_TYPE`, and `CMAKE_CXX_FLAGS_RELEASE`. Require identical benchmark hashes and `-O3 -DNDEBUG` before timing.

- [ ] **Step 2: Run untimed correctness and diagnostic processes.** Run 1024-key `phase=all` at 1/4/8 writers for baseline, isolation, and final. Run separate 131072-key eight-writer diagnostics for each seed and record root/segment attempts, failures, compatible retries, successful local retries, incompatible fallbacks, root restarts, Arena, errors, and hash. Diagnostic rows never enter the timed matrix.

- [ ] **Step 3: Run an isolated three-seed A/B before the final matrix.** Use five eight-writer processes for seeds 20260920-20260922 for baseline, isolation, and final, interleaved with SkipList. Retain isolation only if it passes Arena, does not regress the median in at least two seeds, and improves the worst baseline seed. Retain retry only if it reduces root restarts, does not regress the median in at least two seeds, and improves the worst retained-precursor seed. Revert a failed sub-change with a new revert commit; rebuild the resulting clean final candidate. Do not introduce another representation or publication mechanism.

- [ ] **Step 4: Run the binding five-seed matrix.**

```bash
stability_output="$(mktemp -d /tmp/cedar-32x8-stability.XXXXXX)"
benchmarks/run_cedar_patricia_32x8_stability_matrix.sh \
  --nibble /Volumes/E/CedarBuild/patricia-retention-nibble-o3-20260922/cedar_radix_memtable_bench \
  --byte /Volumes/E/CedarBuild/patricia-retention-byte-o3-20260922/cedar_radix_memtable_bench \
  --baseline /Volumes/E/CedarBuild/patricia-32x8-baseline-o3-20260922/cedar_radix_memtable_bench \
  --candidate /Volumes/E/CedarBuild/patricia-32x8-final-o3-20260922/cedar_radix_memtable_bench \
  --output "$stability_output"
python3 benchmarks/summarize_cedar_patricia_32x8_stability.py \
  --matrix "$stability_output/matrix-raw/matrix.csv" \
  --output "$stability_output/summary.json"
```

Expected: summarizer exits 0; all five seeds pass every ratio, CV, correctness, and Arena gate.

- [ ] **Step 5: Diagnose only implementation defects if a gate fails.** Use the recorded counters and raw samples to distinguish excessive root restart, failed compatible retry, padding/Arena regression, or control variance. Correct only a defect in the approved spacing or one-retry implementation, with a new RED test and full rerun of Steps 2-4. Do not add a third optimization mechanism or change 32-by-8.

- [ ] **Step 6: Write and commit immutable evidence.** The report includes all raw rows, exact medians/CVs, per-seed ratios, Arena, peak RSS, hashes, diagnostic counters, retained/reverted sub-change commits, and the statement that performance optimization is frozen.

```bash
git add -f docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-stability
git commit -m "perf: stabilize 32-by-8 patricia writes"
```

### Task 6: Prove Freeze And Flush/SST Semantics

**Files:**
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/storage/test_rocksdb_lifecycle.cc`

**Interfaces:**
- No production API change.
- MemTable tests compare active and frozen ordered entry vectors and hashes across byte-39 values 0-255, Put/Delete value types, and multiple sequence numbers.
- Lifecycle test uses `OpenRawCedarDatabase`, facts column-family handle 1, `FlushOptions.wait=true`, close/reopen, point reads, forward iteration, and reverse iteration.

- [ ] **Step 1: Add the freeze regression test before any lifecycle claim.** Insert fixed entries whose normalized keys differ at byte 39, capture active forward/reverse scans, call `MarkReadOnly` then `PrepareForFlush` twice, and require both frozen scans to match active count, order, entry bytes, and hash. Require the ready forward scan to add zero `branch_load_for_testing` calls after its initial Seek.

- [ ] **Step 2: Add a real Flush/SST lifecycle test.** Create Put and Delete internal keys for one fact plus a surviving fact, flush facts with `wait=true`, assert at least one live facts SST with Cedar Parquet magic, close/reopen, and compare point lookup plus complete forward/reverse iterator hashes with the pre-flush view.

- [ ] **Step 3: Run RED against the retained code.** The new tests may already pass because the external format is unchanged; if so, record that they are characterization tests and verify they fail under a local negative control that skips `PrepareForFlush` or reverses one expected key. Remove the negative control before continuing.

- [ ] **Step 4: Run the focused Debug suites.**

```bash
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable test_rocksdb_lifecycle -j1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R '^(PartitionedVersionRadixMemTableTest\.|RocksDbLifecycleTest\.)' \
  --output-on-failure
```

Expected: all focused tests pass.

- [ ] **Step 5: Commit only lifecycle characterization tests.**

```bash
git add src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc \
  tests/storage/test_rocksdb_lifecycle.cc
git commit -m "test: cover radix freeze and flush lifecycle"
```

### Task 7: Prove WAL Recovery And Bidirectional Format Compatibility

**Files:**
- Create: `tests/storage/cedar_radix_format_fixture.cc`
- Modify: `tests/CMakeLists.txt`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-full-chain/commands.txt`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-full-chain/compatibility.json`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-full-chain/report.md`

**Interfaces:**
- Fixture CLI: `cedar_radix_format_fixture {write-sst|write-wal|verify} ABS_DB_PATH`.
- The fixed dataset commits multiple versions, a delete, a surviving fact, and keys whose normalized discriminator reaches byte 39. `verify` opens through normal `FactStore` and raw RocksDB paths, validates visible/not-found results, iterates facts in both directions, and prints one stable JSON line containing count, order hash, value hash, and commit sequence.
- `write-sst` waits for a facts flush and closes normally. `write-wal` forks a child that commits then calls `_exit(0)` without destructors; the parent verifies a WAL exists and returns without opening the database.

- [ ] **Step 1: Add a RED target and CLI contract.** Register `cedar_radix_format_fixture` linked to `cedar_core`, then add a CTest script that requires missing/relative paths and unknown modes to exit nonzero. Build before creating the source and require failure.

- [ ] **Step 2: Implement the fixed fixture.** Reuse `FactStoreOptions`, `StoreCommitBatch`, `internal::MakeRocksDbOptions`, and `internal::MakeRocksDbColumnFamilyDescriptors`. Reject an existing output path in write modes, require an existing directory in verify mode, and emit JSON only after every expected row and hash matches.

- [ ] **Step 3: Verify same-binary SST and WAL recovery.** Run final `write-sst -> verify` and `write-wal -> verify` in separate `mktemp -d` parents. Record directory listings before verify; SST case must contain a facts SST, WAL case must contain WAL and no live facts SST before recovery.

- [ ] **Step 4: Commit the fixture separately so it can be applied to the old production tree.**

```bash
git add tests/storage/cedar_radix_format_fixture.cc tests/CMakeLists.txt
git commit -m "test: add radix format compatibility fixture"
```

- [ ] **Step 5: Build identical fixture source against old and final production.** Create detached worktrees at `76adf448257aaaa4eacac8ba5451ba38a51223f3` and final HEAD. Cherry-pick only the Task 7 fixture commit into the old detached worktree, verify production radix/WAL/SST sources still match `76adf44`, and build both fixture targets with Debug settings. Require identical SHA-256 for `cedar_radix_format_fixture.cc`.

- [ ] **Step 6: Run all four cross-version directions.**

```text
old write-sst -> final verify
final write-sst -> old verify
old write-wal -> final verify
final write-wal -> old verify
```

For each direction use a new database directory, record producer/consumer commits and fixture-source hash, and require identical count, order hash, value hash, and commit sequence.

- [ ] **Step 7: Run existing recovery suites plus sanitizers.** Configure separate builds with `-DCEDAR_ENABLE_ASAN=ON` and `-DCEDAR_ENABLE_TSAN=ON`. Build with one job and run:

```bash
cmake -S . -B /Volumes/E/CedarBuild/pure-radix-32x8-asan-20260922 \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DCEDAR_ENABLE_ASAN=ON \
  -DCEDAR_ROCKSDB_CACHE_ROOT=/Volumes/E/CedarRocksDBCache \
  -DCEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
cmake --build /Volumes/E/CedarBuild/pure-radix-32x8-asan-20260922 \
  --target test_partitioned_version_radix_memtable test_rocksdb_lifecycle \
  test_recovery_crash_matrix test_recovery_format -j1
ASAN_OPTIONS=halt_on_error=1 ctest \
  --test-dir /Volumes/E/CedarBuild/pure-radix-32x8-asan-20260922 \
  -R 'PartitionedVersionRadix|RocksDbLifecycle|Recovery' --output-on-failure
cmake -S . -B /Volumes/E/CedarBuild/pure-radix-32x8-tsan-20260922 \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DCEDAR_ENABLE_TSAN=ON \
  -DCEDAR_ROCKSDB_CACHE_ROOT=/Volumes/E/CedarRocksDBCache \
  -DCEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
cmake --build /Volumes/E/CedarBuild/pure-radix-32x8-tsan-20260922 \
  --target test_partitioned_version_radix_memtable test_rocksdb_lifecycle \
  test_recovery_crash_matrix test_recovery_format -j1
TSAN_OPTIONS=halt_on_error=1:report_bugs=1 ctest \
  --test-dir /Volumes/E/CedarBuild/pure-radix-32x8-tsan-20260922 \
  -R 'PartitionedVersionRadix|RocksDbLifecycle|Recovery' --output-on-failure
```

Also run Debug executables `test_rocksdb_lifecycle`, `test_recovery_crash_matrix`, and `test_recovery_format`. Expected: zero test failures and no sanitizer reports.

- [ ] **Step 8: Commit full-chain evidence.** Include exact commands, outputs, file listings, binary commits, source hashes, four compatibility JSON results, sanitizer configuration, and test totals.

```bash
git add -f docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-full-chain
git commit -m "test: verify radix persistence compatibility"
```

### Task 8: Freeze The Design And Audit The Complete Goal

**Files:**
- Modify: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-stability/report.md`
- Modify: `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-full-chain/report.md`

**Interfaces:**
- Final reports name the retained production commit and explicitly mark every goal requirement pass/fail with an evidence path.
- No source changes are allowed after the final Release matrix without rerunning Tasks 5-7.

- [ ] **Step 1: Run the final clean verification set.** Run focused Debug tests, both model negative controls, matrix/summarizer self-tests, `git diff --check`, and confirm the only uncommitted paths are the three user-owned benchmark paths.

- [ ] **Step 2: Build a requirement-to-evidence audit.** Include separate rows for 40-byte/byte-39 correctness, immutable/CAS-only publication, one writer, four writers, all five eight-writer ratios, all five CVs, Arena at 1/4/8 writers, `MarkReadOnly`, `PrepareForFlush`, Flush/SST, SST reopen, WAL replay, old->new SST/WAL, and new->old SST/WAL. A missing or indirect item remains failed.

- [ ] **Step 3: Perform the required fresh whole-branch review.** Package the branch diff from its merge base, the approved spec, this plan, the ledger rulings, Review Focus items, and both evidence reports. Resolve Critical/Important findings in one RED/GREEN fix pass and rerun every affected verification; record Minor findings as residual risks.

- [ ] **Step 4: Commit the final design-freeze reports.**

```bash
git add -f \
  docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-stability/report.md \
  docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-full-chain/report.md
git commit -m "docs: freeze 32-by-8 patricia design"
```

- [ ] **Step 5: Mark the active goal complete only after the audit has no missing or failed requirement.** If any binding item remains unproved, keep the goal active and continue the owning task rather than narrowing completion.
