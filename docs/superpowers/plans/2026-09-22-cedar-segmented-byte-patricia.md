# Cedar Segmented Byte Patricia Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace 4-bit segmented Patricia branches with byte-discriminated, four-segment immutable Patricia branches and measure whether this reduces Cedar's single-writer insertion cost.

**Architecture:** A Branch keeps four atomic immutable child-block edges, but its discriminator is a key byte and each block packs a 64-value local bitmap. A byte's high two bits select an independent CAS segment and its low six bits select a packed child rank. Root/segment publication, monotonic boundaries, normalized keys, and RocksDB interfaces remain unchanged.

**Tech Stack:** C++20, embedded RocksDB 11.1.2, GoogleTest, Python model checkers, CMake, macOS Clang Debug/RelWithDebInfo/ASan/TSan.

**Spec:** `docs/superpowers/specs/2026-09-22-cedar-segmented-byte-patricia-design.md`

## Global Constraints

- Work only in `/Users/wangyang/Desktop/Cedar/.worktrees/pure-radix-cas-cursor` on `codex/pure-radix-cas-cursor`.
- Preserve pure path-compressed Patricia Radix, immutable arena-owned payloads, CAS-only publication, normalized 40-byte keys, and current WAL/recovery/durable-format behavior.
- Do not introduce locks, sharding, workers, staging buffers, Vector/SkipList/ART fallback, or reclamation before MemTable destruction.
- A byte branch owns exactly four atomic `ChildBlock*` segment edges. CAS success is release and failure is acquire; readers acquire-load edges before dereference.
- Keep `phase=index` allocation-free and preserve the benchmark's 28-column stdout CSV.
- Build with exactly one job: `cmake --build <build-dir> -j1`.
- Retain only with exact expected hashes, 131072 ops, zero errors, Debug/ASan/TSan/lifecycle success, 15% or greater one-writer Radix `memtable` median improvement versus 34.976 ms, and no more than 5% B1 regression at 4/8 writers. B0 passes only at `Radix <= 1.10 * Vector`.

## Review Focus

- A byte boundary at 63/64, 127/128, or 191/192 must seek to the same adjacent leaf as a bytewise oracle; Task 3 owns this.
- Direct insertion and a wrapper replacement in different byte segments must both publish; Task 2 owns this interleaving.
- A stale same-segment 64-bit bitmap block must fail CAS, restart, and retain every prior child; Task 2 owns this model and C++ case.
- A distinction only in byte 39 must produce one valid byte branch and preserve duplicate rejection; Task 1 owns this.
- Publication readers must never observe a partially initialized block, and frozen forward cursors must remain branch-load-free; Tasks 3 and 4 own these checks.

### Task 1: Specify Byte-Branch Contracts

**Files:**
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/models/cedar_pure_radix_model.py`
- Modify: `tests/models/cedar_pure_radix_interleaving_model.py`

**Interfaces:**
- Produce test-visible `StructureStatsForTesting` byte segment occupancy/counts and `SegmentForByte(uint8_t) == byte >> 6` after Task 2.
- Model `first_differing_byte(left, right)` returns `[0, 39]` or `40` for equality.

- [ ] **Step 1: Write C++ RED contracts.** Add a standalone-index test inserting all 256 values in byte 39 in scrambled order. Assert `Contains`, cursor order, one byte branch, four byte segments, and exact child counts of 64. Add seeks immediately below/above 63/64, 127/128, and 191/192.
- [ ] **Step 2: Write Python RED contracts.** Model four 64-bit occupancy blocks, packed ranks, and stale same-segment replacement. Its negative control must unconditionally publish the stale block and lose a child.
- [ ] **Step 3: Verify RED.** Run both models with `--check-negative-control`; build the focused target and run `ctest -R 'BytePatricia|ByteSegment'`. Expected: C++ compile/test failure because the current nibble representation has no byte block contract.
- [ ] **Step 4: Commit RED contracts.** Force-add ignored evidence only when creating it; do not stage `benchmarks/run_cedar_radix_read_matrix.sh`.

### Task 2: Publish Immutable Byte Blocks

**Files:**
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/models/cedar_pure_radix_interleaving_model.py`

**Interfaces:**
- Replace `kNibbles`, `NibbleAt`, and `SegmentFor` with `kKeyBytes`, `ByteAt`, and `SegmentForByte`.
- `ChildBlock { uint64_t occupied; uint8_t child_count; Node* children[1]; }` packs children in low-six-bit byte order.
- `CopyBlockWithInsertedChild`, `CopyBlockWithReplacedChild`, and `ReplaceBlock(Branch*, uint8_t, ChildBlock*, ChildBlock*)` retain restart-on-failure behavior.

- [ ] **Step 1: Implement exact-size bitmap primitives.** Allocate `offsetof(ChildBlock, children) + popcount(occupied) * sizeof(Node*)`; use `std::popcount`, `std::countr_zero`, and `std::countl_zero` on `uint64_t`; assert the local value is below 64.
- [ ] **Step 2: Replace write descent and collision construction.** Record byte frames, create two-child byte branches at `FirstDifferingByte`, and copy/CAS only the selected segment. Fully initialize every new Branch/block before root or parent publication.
- [ ] **Step 3: Add RED interleavings before changing publication.** Pause a direct byte insertion in segment 1 while another writer wraps a child in segment 3; assert both survive. Add a same-segment stale snapshot test that requires three CAS observer calls.
- [ ] **Step 4: Verify GREEN.** Run focused byte tests, `PartitionedVersionRadix`, and both models with negative controls. Commit `perf: publish segmented byte patricia blocks`.

### Task 3: Port Ordered Reads And Frozen Traversal

**Files:**
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`

**Interfaces:**
- `FirstChildAtOrAfter(const Branch*, uint8_t)` and `LastChildAtOrBefore(const Branch*, uint8_t)` return a byte value or 256.
- Cursor frame direction storage changes from nibble to byte; capacity is `kKeyBytes`.

- [ ] **Step 1: Write RED ordered-read tests.** Cover byte segment boundaries, sparse first/last bytes, `Seek`/`SeekForPrev`, byte-39 order, deep 39-byte paths, and randomized iteration parity versus SkipList.
- [ ] **Step 2: Port `Contains`, min/max, cursor descent/backtracking, seek bounds, and frozen-chain traversal.** Every selected edge remains acquire-loaded; a ready frozen cursor follows only `frozen_next`.
- [ ] **Step 3: Verify GREEN.** Run all Debug `PartitionedVersionRadix` tests and the sequential model negative control. Commit `perf: traverse segmented byte patricia blocks`.

### Task 4: Validate Safety And Measure The Representation

**Files:**
- Modify: `benchmarks/cedar_radix_memtable_bench.cc`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-segmented-byte-patricia/matrix.csv`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-segmented-byte-patricia/report.md`

**Interfaces:**
- Keep stdout unchanged; use stderr-only diagnostics named `byte_segment_snapshot_cas` when `--stats` is selected.

- [ ] **Step 1: Write RED publication/CSV contracts.** Readers repeatedly seek while writers cover all byte segments and race duplicate insertion; assert count/hash/order after joins. Assert `--stats` stderr emits the byte diagnostic while stdout retains 28 columns.
- [ ] **Step 2: Run Debug, ASan, TSan, model, and lifecycle verification.** Use `halt_on_error=1`; run direct lifecycle (11), crash recovery (6), and format recovery (4) executables after all focused suites are green.
- [ ] **Step 3: Build Release and collect evidence.** Run five independent alternating processes at 131072 random keys, seed `20260921`: Vector/Radix for 1-writer `index` and `memtable`; SkipList/Radix for 4/8-writer `memtable`. Reject any row without 28 columns, expected operations/hash, or zero errors.
- [ ] **Step 4: Apply gates and commit/revert.** Retain only at the stated 15%/B1/safety gates. If retained, force-add raw evidence and commit `perf: measure segmented byte patricia`; otherwise restore all production and byte-specific test changes, record the exact ruling in the SDD ledger, and leave this goal active unless B0 passes.
