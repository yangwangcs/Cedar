# Cedar Concurrent Patricia Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Select and retain a pure immutable Patricia representation that preserves current byte-Patricia single-writer speed, restores a clear four/eight-writer advantage over SkipList, limits Arena use, and remains correct from active MemTable writes through freeze, Flush/SST, and restart recovery.

**Architecture:** First measure the last segmented-nibble production source and the current four-by-64 segmented-byte source with identical committed benchmark code and identical Release flags. Retain nibble immediately if every gate passes; only when nibble fails solely at the single-writer gate, implement a 16-by-16 segmented-byte candidate and subject it to the same matrix. The selected in-memory representation keeps immutable arena-owned nodes and CAS-only publication; cross-version fixture binaries prove that no WAL/SST format dependency leaked into the representation change.

**Tech Stack:** C++20, embedded RocksDB 11.1.2, GoogleTest, Python 3 standard-library CSV/statistics tooling, Bash, CMake, macOS Clang Release/Debug/ASan/TSan.

**Spec:** `docs/superpowers/specs/2026-09-22-cedar-concurrent-patricia-retention-design.md`

## Global Constraints

- Work in `/Users/wangyang/Desktop/Cedar/.worktrees/pure-radix-cas-cursor` on `codex/pure-radix-cas-cursor`; use additional detached worktrees only for clean comparison builds.
- Preserve the existing modifications in `benchmarks/cedar_radix_memtable_bench.cc` and `tests/performance/test_radix_benchmark_csv.cmake`, and the untracked `benchmarks/run_cedar_radix_read_matrix.sh`; do not edit, stage, or commit them.
- The Nibble production source is `0cd5d86`; use build ref `77593a8`, which differs only by a 27-line test and has identical production and benchmark sources. The four-by-64 Byte source is `76adf44`.
- Before timing, require `git diff --quiet 77593a8 76adf44 -- benchmarks/cedar_radix_memtable_bench.cc`; both comparison builds use `Release`, `-O3 -DNDEBUG`, `/usr/bin/c++`, Unix Makefiles, and `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1`.
- Keep one pure path-compressed Patricia tree over all 40 normalized key bytes. Byte 39 remains a discriminator; do not introduce a 39-byte shortcut.
- Published leaves, branches, and child blocks are immutable. Root/segment publication remains release CAS; failure and reader edge loads remain acquire. Arena allocations live until MemTable destruction.
- Do not add locks, shards, background workers, staging buffers, reclamation, or Vector/SkipList/ART fallback paths.
- Do not change MemTableRep, WAL, commit, Flush/SST, recovery, or durable-format contracts. `ExternalMemoryUsage()` remains zero because the owning Arena accounts storage.
- Performance runs use no `--stats`; diagnostic runs are separate and untimed. Standard benchmark stdout remains exactly 28 CSV fields.
- Production matrices use 131072 random keys, seeds `20260920`, `20260921`, and `20260922`, five independent processes per seed/configuration, alternating implementation order, 131072 operations, zero errors, and matching hashes within each seed/phase/writer group.
- Apply every retention ratio to each seed's five-run median. One writer is `candidate <= 1.05 * four-by-64 Byte`; four writers are `candidate <= 0.80 * SkipList`; eight writers are `candidate <= 0.85 * SkipList`; Arena at each writer count is `candidate <= 1.25 * Nibble`.
- Report the historical `Radix <= 1.10 * Vector` single-writer criterion separately. Do not mark that historical goal passed unless the new matrix proves it.
- Configure and build with one job. Use `cmake --build <dir> --target <target> -j1`.

## Review Focus

- A fast Nibble result built with O2 or a different benchmark is invalid; Task 2 checks compiler flags, benchmark blob identity, and source revisions before collecting samples.
- Median aggregation across all three seeds can hide a bad seed; Tasks 1, 2, and 6 require every per-seed median to pass independently.
- A same-segment stale replacement can silently drop a child while cross-segment tests remain green; Task 4 adds a forced stale-CAS negative control and Task 5 retains restart-from-root semantics.
- Segment transitions at 15/16 through 239/240 can break ordered Seek/SeekForPrev without breaking Contains; Tasks 4 and 5 cover every boundary in both directions.
- A representation can pass MemTable tests but fail at freeze or after persistence; Task 7 covers MarkReadOnly/PrepareForFlush and Task 8 performs bidirectional old/selected WAL and SST compatibility checks.

---

### Task 1: Add A/B Matrix And Gate Tooling

**Files:**
- Create: `benchmarks/run_cedar_patricia_retention_matrix.sh`
- Create: `benchmarks/summarize_cedar_patricia_retention.py`
- Create: `tests/performance/test_patricia_retention_matrix.cmake`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- `run_cedar_patricia_retention_matrix.sh --nibble ABS_BIN --byte ABS_BIN [--candidate ABS_BIN] --output ABS_EMPTY_DIR` writes `matrix.csv` plus one two-line raw CSV file per process. `CEDAR_RETENTION_TEST_MODE=1` changes only entries/seeds/repeats to `64/20260920/1` for the CTest contract.
- Matrix rows prefix the benchmark's unchanged 28 fields with `variant`, where variant is `nibble`, `byte`, `candidate`, `skiplist`, or `vector`.
- `summarize_cedar_patricia_retention.py --matrix PATH --candidate {nibble,candidate} --output PATH` writes deterministic JSON. Exit 0 means all gates pass, 10 means only the one-writer time gate fails, and 11 means a correctness, concurrency, Arena, or sample-completeness gate fails. `--self-test` runs in-memory pass, single-writer-only failure, hard-gate failure, and malformed-sample cases without reading a matrix.

- [ ] **Step 1: Add the failing CMake contract.** Register `PatriciaRetentionMatrixContract` only when `cedar_radix_memtable_bench` exists:

```cmake
add_test(NAME PatriciaRetentionMatrixContract COMMAND ${CMAKE_COMMAND}
    -DCEDAR_SOURCE_DIR=${CMAKE_SOURCE_DIR}
    -DCEDAR_RADIX_BENCHMARK=$<TARGET_FILE:cedar_radix_memtable_bench>
    -P ${CMAKE_CURRENT_SOURCE_DIR}/performance/test_patricia_retention_matrix.cmake)
```

The test creates an empty temporary directory, runs the matrix script with the
same benchmark as Nibble and Byte under `CEDAR_RETENTION_TEST_MODE=1`, and
asserts: every raw row has 28 fields; every matrix row has 29 fields; stdout
contains no `radix_stats`; each group has one sample; operations are 64;
errors are zero; and all group hashes match.

- [ ] **Step 2: Run the contract to verify RED.**

```bash
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 --target cedar_radix_memtable_bench -j1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 -R '^PatriciaRetentionMatrixContract$' --output-on-failure
```

Expected: FAIL because the runner and summarizer do not exist.

- [ ] **Step 3: Implement strict argument and row validation in the runner.** Use fixed production arrays and keep diagnostics off:

```bash
entries=131072
seeds=(20260920 20260921 20260922)
repeats=(1 2 3 4 5)
[[ "${CEDAR_RETENTION_TEST_MODE:-0}" == 1 ]] && {
  entries=64
  seeds=(20260920)
  repeats=(1)
}

run_one() {
  local variant="$1" binary="$2" implementation="$3"
  local seed="$4" writers="$5" phase="$6" repeat="$7" raw="$8"
  "$binary" --implementation "$implementation" --entries "$entries" \
    --workload random --writers "$writers" --seed "$seed" \
    --revision "$variant" --phase "$phase" > "$raw"
  [[ "$(awk -F, 'NR == 2 { print NF }' "$raw")" == 28 ]]
  [[ "$(awk -F, 'NR == 2 { print $9 }' "$raw")" == "$entries" ]]
  [[ "$(awk -F, 'NR == 2 { print $17 }' "$raw")" == 0 ]]
  printf '%s,%s\n' "$variant" "$(sed -n '2p' "$raw")" >> "$output/matrix.csv"
}
```

Run Nibble/Byte and a Vector control from the Byte binary at one-writer
`index`; Nibble/Byte and the same Vector control at one-writer `memtable`;
and Nibble/Byte plus a SkipList control from the Byte binary at four/eight-
writer `memtable`. When
`--candidate` is present, add Candidate to the corresponding Radix groups.
Alternate the complete variant order by `(seed + writers + repeat) % 2`.
Reject a nonempty output directory, a non-executable/non-absolute binary,
missing rows, mismatched phase/writer fields, or mismatched hashes.

- [ ] **Step 4: Implement structured per-seed gate evaluation.** Parse with `csv.DictReader`, group by `(variant, seed, phase, writers)`, require exactly five production samples, and use `statistics.median`. Emit this stable shape:

```python
result = {
    "candidate": args.candidate,
    "seeds": {},
    "all_gates_pass": False,
    "only_single_writer_failed": False,
}

for seed in (20260920, 20260921, 20260922):
    one = median_ns(args.candidate, seed, "memtable", 1)
    byte = median_ns("byte", seed, "memtable", 1)
    four = median_ns(args.candidate, seed, "memtable", 4)
    four_skip = median_ns("skiplist", seed, "memtable", 4)
    eight = median_ns(args.candidate, seed, "memtable", 8)
    eight_skip = median_ns("skiplist", seed, "memtable", 8)
    arena_ok = all(
        median_arena(args.candidate, seed, writers)
        <= 1.25 * median_arena("nibble", seed, writers)
        for writers in (1, 4, 8)
    )
```

Use ratios `one/byte`, `four/four_skip`, and `eight/eight_skip`, compare with
1.05/0.80/0.85, record Vector separately, and never round before deciding.
Malformed rows, sample-count mismatch, nonzero errors, operations mismatch,
or hash mismatch exit 11 with a precise stderr message.

The `--self-test` path constructs five samples for all required groups and
asserts the evaluator returns: 0 for ratios 1.00/0.70/0.75 and Arena 1.00;
10 after changing only one-writer ratio to 1.06; 11 after changing four-writer
ratio to 0.81; and 11 after deleting one sample.

- [ ] **Step 5: Verify GREEN, syntax, and failure modes.**

```bash
bash -n benchmarks/run_cedar_patricia_retention_matrix.sh
python3 -m py_compile benchmarks/summarize_cedar_patricia_retention.py
python3 benchmarks/summarize_cedar_patricia_retention.py --self-test
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 -R '^PatriciaRetentionMatrixContract$' --output-on-failure
```

Also pass a deliberately truncated copy of the test matrix to the summarizer
and require exit 11 with `sample count` in stderr.

- [ ] **Step 6: Commit only the new tooling and CMake registration.**

```bash
git add benchmarks/run_cedar_patricia_retention_matrix.sh \
  benchmarks/summarize_cedar_patricia_retention.py \
  tests/performance/test_patricia_retention_matrix.cmake tests/CMakeLists.txt
git commit -m "bench: add patricia retention matrix"
```

### Task 2: Measure Nibble Against Four-By-64 Byte Under One Release Configuration

**Files:**
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-retention-ab/matrix-raw/matrix.csv`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-retention-ab/summary.json`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-retention-ab/report.md`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-retention-ab/matrix-raw/*.csv`

**Interfaces:**
- Clean detached source worktrees: `.worktrees/patricia-retention-nibble-o3` at `77593a8` and `.worktrees/patricia-retention-byte-o3` at `76adf44`.
- Clean build directories: `/Volumes/E/CedarBuild/patricia-retention-nibble-o3-20260922` and `/Volumes/E/CedarBuild/patricia-retention-byte-o3-20260922`.
- The Nibble summary exit code is the branch decision: 0 -> Task 3; 10 -> skip Task 3 and run Tasks 4-6; 11 -> investigate/report and stop without implementing 16-by-16.

- [ ] **Step 1: Prove source and benchmark comparability before building.**

```bash
git diff --quiet 77593a8 76adf44 -- benchmarks/cedar_radix_memtable_bench.cc
git diff --quiet 0cd5d86 77593a8 -- \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.h \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.cc \
  benchmarks/cedar_radix_memtable_bench.cc
git show -s --format='%H %s' 77593a8 76adf44
```

Expected: both `diff --quiet` commands return 0; record both full hashes.

- [ ] **Step 2: Create clean detached worktrees and identical Release builds.** Follow `using-git-worktrees` safety checks, then configure each source with:

```bash
cmake -S <SOURCE> -B <BUILD> -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_BENCHMARKS=ON -DBUILD_TESTS=ON \
  -DCEDAR_ROCKSDB_CACHE_ROOT=/Volumes/E/CedarRocksDBCache \
  -DCEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
cmake --build <BUILD> --target cedar_radix_memtable_bench -j1
```

Verify each `CMakeCache.txt` contains `CMAKE_BUILD_TYPE:STRING=Release`,
`CMAKE_CXX_FLAGS_RELEASE:STRING=-O3 -DNDEBUG`, and
`CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/c++`. Hash both checked-out benchmark
sources and require identical SHA-256 values.

- [ ] **Step 3: Run one untimed correctness smoke and one diagnostic process.** For both binaries, run 1024 random keys for writers 1/4/8 with `phase=all`, then one 131072-key `--stats` process. Require 28 stdout fields, zero errors, equal hashes, and record stderr CAS/boundary counters. Do not reuse these rows as performance samples.

- [ ] **Step 4: Collect the production matrix without diagnostics.**

```bash
benchmarks/run_cedar_patricia_retention_matrix.sh \
  --nibble /Volumes/E/CedarBuild/patricia-retention-nibble-o3-20260922/cedar_radix_memtable_bench \
  --byte /Volumes/E/CedarBuild/patricia-retention-byte-o3-20260922/cedar_radix_memtable_bench \
  --output /absolute/empty/evidence/matrix-raw
```

Do not set `CEDAR_RETENTION_TEST_MODE` and do not pass `--stats`.

- [ ] **Step 5: Apply the gates and write the report before changing representation code.**

```bash
python3 benchmarks/summarize_cedar_patricia_retention.py \
  --matrix /absolute/evidence/matrix-raw/matrix.csv \
  --candidate nibble --output /absolute/evidence/summary.json
```

Record per-seed medians and ratios, Arena and peak RSS, source hashes, flags,
raw-row validation, diagnostic counters, and the exact exit code. State
explicitly whether Vector's historical 1.10x criterion passed. Do not average
seeds to reverse a failed per-seed gate.

- [ ] **Step 6: Commit immutable evidence.** Force-add only the new ignored evidence directory and preserve the three pre-existing dirty paths.

```bash
git add -f docs/superpowers/evidence/2026-09-22-cedar-patricia-retention-ab
git commit -m "perf: compare nibble and byte patricia"
```

### Task 3: Retain The Nibble Representation When It Passes

**Condition:** Run only when Task 2's summarizer exits 0.

**Files:**
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/models/cedar_pure_radix_model.py`
- Modify: `tests/models/cedar_pure_radix_interleaving_model.py`

**Interfaces:**
- Restore the production representation at `0cd5d86` and the validation state at `77593a8`: `kNibbles = 80`, `uint8_t ChildBlock::occupied`, four atomic segments, `SegmentFor(nibble) == nibble >> 2`, and `LocalNibbleFor(nibble) == nibble & 3`.
- Keep the path-frame tail uninitialized optimization and the standalone entry-pointer contract.

- [ ] **Step 1: Change the representation contract test first.** Add/restore a test named `SegmentedNibbleFinalByteUsesFourPackedSegments` that inserts keys covering all 16 nibbles in both halves of byte 39 and asserts nibble depth, four segments, local rank order, Contains, Seek, and SeekForPrev.

```c++
EXPECT_EQ(CedarPureRadixIndex::kNibbles, 80U);
EXPECT_EQ(stats.segment_child_counts[0], 4U);
EXPECT_EQ(stats.segment_child_counts[1], 4U);
EXPECT_EQ(stats.segment_child_counts[2], 4U);
EXPECT_EQ(stats.segment_child_counts[3], 4U);
```

- [ ] **Step 2: Verify RED against current Byte.** Build the focused test with one job and run the new test. Expected: compile failure because the current public contract has no `kNibbles`, or assertion failure against the Byte structure.

- [ ] **Step 3: Restore the Nibble representation and models without overwriting the RED test.** Use `git diff 77593a8..76adf44` as the review oracle. Apply its inverse with `apply_patch` to the production header/source and both model files. In the C++ test file, remove Byte-only structure/interleaving expectations and restore their Nibble equivalents while preserving the new `SegmentedNibbleFinalByteUsesFourPackedSegments` test from Step 1 and the standalone entry-pointer contract. Preserve changes outside the five listed files. The target types are:

```c++
struct ChildBlock {
  uint8_t occupied = 0;
  uint8_t child_count = 0;
  Node* children[1];
};

struct Branch final : Node {
  uint8_t nibble_index = 0;
  std::array<std::atomic<ChildBlock*>, 4> segments{};
  std::atomic<Leaf*> min_leaf{nullptr};
  std::atomic<Leaf*> max_leaf{nullptr};
};
```

Every path frame records a nibble; collisions use `FirstDifferingNibble`;
successor/predecessor scans remain globally ordered across four segments.

- [ ] **Step 4: Verify GREEN before selection.**

```bash
python3 tests/models/cedar_pure_radix_model.py --check-negative-control
python3 tests/models/cedar_pure_radix_interleaving_model.py --check-negative-control
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R 'PartitionedVersionRadix' --output-on-failure
```

- [ ] **Step 5: Commit the selected representation.** Stage only the five listed files.

```bash
git commit -m "perf: retain segmented nibble patricia" -- \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.h \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.cc \
  src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc \
  tests/models/cedar_pure_radix_model.py \
  tests/models/cedar_pure_radix_interleaving_model.py
```

Then skip Tasks 4-6 and continue at Task 7.

### Task 4: Specify Sixteen-By-Sixteen Byte Publication

**Condition:** Run only when Task 2 exits 10, meaning Nibble failed only the one-writer gate.

**Files:**
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/models/cedar_pure_radix_model.py`
- Modify: `tests/models/cedar_pure_radix_interleaving_model.py`

**Interfaces:**
- Test-visible contract: `kByteSegmentCount == 16`, `SegmentForByte(value) == value >> 4`, local byte `value & 0x0f`, 16-bit occupancy, and at most 16 packed children per immutable block.
- The interleaving model exposes `publish_if_observed(segment, observed, replacement) -> bool`; stale same-segment publication must return false.

- [ ] **Step 1: Write RED structure and order tests.** Replace four-by-64 expectations with `BytePatriciaFinalByteValuesUseSixteenPackedSegments`. Insert all values 0-255 in byte 39 in scrambled order and assert 16 segments with 16 children each. Add `SixteenByteSegmentsSeekAcrossEveryBoundary`, covering both sides of 15/16, 31/32, ..., 239/240 with Seek and SeekForPrev.

```c++
for (size_t segment = 0; segment < 16; ++segment) {
  EXPECT_EQ(stats.byte_segment_child_counts[segment], 16U);
  EXPECT_EQ(stats.byte_segment_occupied[segment], 0xffffU);
}
```

- [ ] **Step 2: Write RED publication tests.** Force a direct insert in segment 1 to pause before CAS while a wrapper publishes in segment 13; both must survive without sharing an edge. Force two replacements in segment 7 from the same observed block; the stale writer must fail, restart, and retain all three children.

- [ ] **Step 3: Update both Python models and retain negative controls.** The sequential model uses 16 occupancy bits and global rank `segment * 16 + local`. The interleaving negative control unconditionally installs a stale same-segment block and must report a lost key; the real model CASes against the observed block and must preserve it.

- [ ] **Step 4: Verify RED.**

```bash
python3 tests/models/cedar_pure_radix_model.py --check-negative-control
python3 tests/models/cedar_pure_radix_interleaving_model.py --check-negative-control
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
```

Expected: model assertions and C++ compile/tests fail because current code has four 64-value segments.

- [ ] **Step 5: Commit RED contracts only.**

```bash
git commit -m "test: specify sixteen-segment byte patricia" -- \
  src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc \
  tests/models/cedar_pure_radix_model.py \
  tests/models/cedar_pure_radix_interleaving_model.py
```

### Task 5: Implement Sixteen-By-Sixteen Immutable Byte Blocks

**Condition:** Task 4 was selected and its RED failures match the missing representation.

**Files:**
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`

**Interfaces:**
- `static constexpr size_t kByteSegmentCount = 16`.
- `SegmentForByte(uint8_t) -> uint8_t` shifts four bits; `LocalByte(uint8_t) -> uint8_t` masks four bits.
- `ChildBlock::occupied` is `uint16_t`; `FirstChildAtOrAfter` and `LastChildAtOrBefore` return `uint16_t`, using 256 as the no-child sentinel.

- [ ] **Step 1: Change the header representation and exact-size allocation contract.**

```c++
static constexpr size_t kByteSegmentCount = 16;
static constexpr uint8_t SegmentForByte(uint8_t value) { return value >> 4; }

struct ChildBlock {
  uint16_t occupied = 0;
  uint8_t child_count = 0;
  Node* children[1];
};

struct Branch final : Node {
  Branch() : Node(NodeKind::kBranch) {}
  uint8_t byte_index = 0;
  std::array<std::atomic<ChildBlock*>, kByteSegmentCount> segments{};
  std::atomic<Leaf*> min_leaf{nullptr};
  std::atomic<Leaf*> max_leaf{nullptr};
};
```

Update test stats arrays to 16 entries. Allocate exactly
`offsetof(ChildBlock, children) + popcount(occupied) * sizeof(Node*)`.

- [ ] **Step 2: Port bitmap rank and ordered segment scanning.** Use `uint16_t{1} << local`, calculate rank from bits below local, and scan segments 0..15 or 15..0. Never shift by 16. Return global byte `segment * 16 + local`; return 256 only when all eligible blocks are empty.

- [ ] **Step 3: Preserve CAS publication semantics.** Direct insert copies only its observed 16-value block. Wrapper replacement copies only its observed parent block. `ReplaceBlock` release-CASes exactly one segment; failure uses acquire and restarts from an acquire root load. Fully initialize new branches and their one/two initial blocks before parent/root release publication.

- [ ] **Step 4: Preserve all read and freeze paths.** Update Contains, insertion descent, Minimum/MaximumLeaf, Seek, SeekForPrev, cursor backtracking, and frozen-chain construction to the same `segment/local` mapping. Keep cursor capacity at 40 byte branches. A ready forward frozen cursor follows only `frozen_next`.

- [ ] **Step 5: Verify GREEN with models and focused Debug tests.**

```bash
python3 tests/models/cedar_pure_radix_model.py --check-negative-control
python3 tests/models/cedar_pure_radix_interleaving_model.py --check-negative-control
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable -j1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R 'PartitionedVersionRadix' --output-on-failure
```

- [ ] **Step 6: Commit production GREEN.**

```bash
git commit -m "perf: publish sixteen-segment byte patricia" -- \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.h \
  src/engine/rocksdb/memtable/cedar_pure_radix_index.cc
```

### Task 6: Measure And Retain Or Reject Sixteen-By-Sixteen

**Condition:** Tasks 4-5 were selected.

**Files:**
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-16x16/matrix-raw/matrix.csv`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-16x16/summary.json`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-16x16/report.md`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-16x16/matrix-raw/*.csv`

**Interfaces:**
- Build the committed 16-by-16 candidate in a clean detached worktree so the three pre-existing dirty benchmark paths cannot enter the binary.
- Summarizer candidate is `candidate`; controls are the exact Nibble and four-by-64 binaries from Task 2.

- [ ] **Step 1: Create a clean candidate worktree/build and verify flags/source.** Configure with the exact Task 2 Release command, build only `cedar_radix_memtable_bench`, verify `-O3 -DNDEBUG`, and require the benchmark source SHA-256 to equal both Task 2 sources.

- [ ] **Step 2: Run correctness smoke and untimed diagnostics.** Use 1024 random keys at writers 1/4/8 with `phase=all`, plus one 131072-key `--stats` process. Record CAS attempts and Arena. Explain copied-block upper bounds (16 candidate, 64 control) as representation facts, not measured successful-copy counts.

- [ ] **Step 3: Run the production matrix.**

```bash
benchmarks/run_cedar_patricia_retention_matrix.sh \
  --nibble /Volumes/E/CedarBuild/patricia-retention-nibble-o3-20260922/cedar_radix_memtable_bench \
  --byte /Volumes/E/CedarBuild/patricia-retention-byte-o3-20260922/cedar_radix_memtable_bench \
  --candidate /Volumes/E/CedarBuild/patricia-retention-16x16-o3-20260922/cedar_radix_memtable_bench \
  --output /absolute/empty/evidence/matrix-raw
python3 benchmarks/summarize_cedar_patricia_retention.py \
  --matrix /absolute/evidence/matrix-raw/matrix.csv \
  --candidate candidate --output /absolute/evidence/summary.json
```

- [ ] **Step 4: Apply all gates per seed.** Retain only on exit 0. On any failure, write the exact failed ratios and preserve evidence, then revert the two 16-by-16 implementation commits with new revert commits; do not discard unrelated work or claim completion. A failed candidate returns the goal to design review rather than authorizing another representation.

- [ ] **Step 5: Commit evidence and the decision.**

```bash
git add -f docs/superpowers/evidence/2026-09-22-cedar-patricia-16x16
git commit -m "perf: measure sixteen-segment byte patricia"
```

Continue to Task 7 only if the candidate was retained.

### Task 7: Pin Freeze And Forty-Byte Lifecycle Contracts

**Files:**
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/storage/test_rocksdb_lifecycle.cc`

**Interfaces:**
- The selected representation's active iterator, first frozen iterator, and ready frozen iterator return the same ordered hash.
- The lifecycle test uses a canonical 32-byte V2 user key; RocksDB adds its 8-byte trailer, producing the exact normalized 40-byte key consumed by Patricia.

- [ ] **Step 1: Add the selected-representation freeze contract.** Insert keys that exercise the selected segment boundaries and byte 39, collect active order/hash, call `MarkReadOnly()` and `PrepareForFlush()`, then collect two frozen cursors. Assert equal counts, order, and hashes; assert the ready forward cursor adds zero branch-load observations after Seek.

- [ ] **Step 2: Add a Put/Delete last-byte lifecycle regression.** In `RocksDbLifecycleTest.FlushReopenPreservesPutDeleteInternalKeyDiscriminators`, open the raw Cedar DB, write one canonical `EncodeFactKey(...)` as a value, delete the same user key, write a second surviving key, flush facts with `wait=true`, close, reopen, and assert the deleted key is NotFound while the survivor retains its value. The Put and Delete internal entries share 32 user bytes and differ in normalized byte 39 through their RocksDB value types.

```c++
ASSERT_TRUE(database->Put(write_options, handles[1], deleted_key, "old").ok());
ASSERT_TRUE(database->Delete(write_options, handles[1], deleted_key).ok());
ASSERT_TRUE(database->Put(write_options, handles[1], survivor_key, "live").ok());
rocksdb::FlushOptions flush;
flush.wait = true;
ASSERT_TRUE(database->Flush(flush, handles[1]).ok());
```

- [ ] **Step 3: Run focused Debug tests.**

```bash
cmake --build /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  --target test_partitioned_version_radix_memtable test_rocksdb_lifecycle -j1
ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 \
  -R 'PartitionedVersionRadix|RocksDbLifecycleTest.FlushReopenPreservesPutDeleteInternalKeyDiscriminators' \
  --output-on-failure
```

- [ ] **Step 4: Commit lifecycle contracts.**

```bash
git commit -m "test: pin radix freeze and flush lifecycle" -- \
  src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc \
  tests/storage/test_rocksdb_lifecycle.cc
```

### Task 8: Prove Sanitizer, WAL/SST Compatibility, And Final Retention

**Files:**
- Create: `tests/storage/cedar_radix_format_fixture.cc`
- Modify: `tests/CMakeLists.txt`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-full-chain/report.md`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-patricia-full-chain/commands.txt`

**Interfaces:**
- `cedar_radix_format_fixture write-sst ABS_DB`, `write-wal ABS_DB`, and `verify ABS_DB` use fixed facts and exit nonzero on any mismatch.
- Build the identical fixture source twice: once linked to clean four-by-64 control `76adf44`, once linked to the selected committed representation.

- [ ] **Step 1: Add the cross-version fixture executable.** Reuse `FactStore`, `Batch`, and raw facts-column Flush patterns from `test_rocksdb_lifecycle.cc`. `write-sst` commits vertices 101 and 202, closes, reopens raw, flushes facts with `wait=true`, and closes. `write-wal` commits the same facts and terminates with `std::_Exit(0)` before destructors close the DB. `verify` opens normally, asserts commit sequence 2, reads both vertices at valid time 1000, and closes cleanly. Reject relative paths and pre-existing target directories in write modes.

- [ ] **Step 2: Verify fixture behavior within one build.** Build `cedar_radix_format_fixture`, create separate temporary SST and WAL directories, verify each with the same binary, and assert the WAL case contains no facts SST before verification.

- [ ] **Step 3: Run Debug, ASan, and TSan focused suites from fresh selected-source builds.**

```bash
cmake -S <SELECTED_SOURCE> -B <DEBUG_BUILD> -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON \
  -DCEDAR_ROCKSDB_CACHE_ROOT=/Volumes/E/CedarRocksDBCache \
  -DCEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
cmake -S <SELECTED_SOURCE> -B <ASAN_BUILD> -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTS=ON -DCEDAR_ENABLE_ASAN=ON \
  -DCEDAR_ROCKSDB_CACHE_ROOT=/Volumes/E/CedarRocksDBCache \
  -DCEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
cmake -S <SELECTED_SOURCE> -B <TSAN_BUILD> -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTS=ON -DCEDAR_ENABLE_TSAN=ON \
  -DCEDAR_ROCKSDB_CACHE_ROOT=/Volumes/E/CedarRocksDBCache \
  -DCEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
```

Build `test_partitioned_version_radix_memtable` with `-j1` in all three.
Run all `PartitionedVersionRadix` tests; ASan uses
`ASAN_OPTIONS=halt_on_error=1`, TSan uses
`TSAN_OPTIONS=halt_on_error=1:report_bugs=1`. Any sanitizer report fails.

- [ ] **Step 4: Run the full direct lifecycle suites.** Build and execute these binaries directly to avoid the known GoogleTest 1.17 CMake discovery issue:

```bash
<DEBUG_BUILD>/tests/test_rocksdb_lifecycle --gtest_brief=1
<DEBUG_BUILD>/tests/test_recovery_crash_matrix --gtest_brief=1
<DEBUG_BUILD>/tests/test_recovery_format --gtest_brief=1
```

Expected coverage is at least the existing 11 lifecycle, 6 crash, and 4
format cases plus the new lifecycle case, all passing.

- [ ] **Step 5: Prove compatibility in both directions.** Add the identical fixture source/CMake registration as a test-only commit to a detached `76adf44` control worktree and build it there. Use explicit temporary directories and run:

```bash
<CONTROL_FIXTURE> write-sst <CONTROL_SST_DB>
<SELECTED_FIXTURE> verify <CONTROL_SST_DB>
<CONTROL_FIXTURE> write-wal <CONTROL_WAL_DB>
<SELECTED_FIXTURE> verify <CONTROL_WAL_DB>
<SELECTED_FIXTURE> write-sst <SELECTED_SST_DB>
<CONTROL_FIXTURE> verify <SELECTED_SST_DB>
<SELECTED_FIXTURE> write-wal <SELECTED_WAL_DB>
<CONTROL_FIXTURE> verify <SELECTED_WAL_DB>
```

All four verifications must pass. Record producer/verifier full commits and
directory file listings before verification. Remove temporary fixture DBs
only after the command log and results are captured.

- [ ] **Step 6: Write the requirement-by-requirement final report.** Include selected representation, all per-seed performance/Arena gates, Vector status, 40-byte/byte-39 evidence, publication/model results, Debug/ASan/TSan counts, MarkReadOnly/PrepareForFlush behavior, Flush/SST result, restart/WAL recovery, bidirectional format compatibility, source/build hashes, and remaining risks. Every claim links to raw evidence or a command result.

- [ ] **Step 7: Commit fixture and full-chain evidence without dirty-path leakage.**

```bash
git add tests/storage/cedar_radix_format_fixture.cc tests/CMakeLists.txt
git add -f docs/superpowers/evidence/2026-09-22-cedar-patricia-full-chain
git commit -m "test: verify patricia persistence compatibility"
```

- [ ] **Step 8: Perform the completion audit.** Confirm `git diff --check`, the three pre-existing dirty paths remain exactly as found, all retained gates pass, and every spec requirement has direct evidence. Only then mark the active goal complete. If any performance or full-chain gate fails, keep the goal active and report the exact unmet requirement.
