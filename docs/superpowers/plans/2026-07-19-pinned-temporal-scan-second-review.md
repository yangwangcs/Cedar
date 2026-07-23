# Pinned Temporal Scan Second Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close all four second-review findings with bounded I/O, complete memory accounting, and exact attempt observability.

**Architecture:** Keep the existing pinned source merger and add ownership at the boundaries where state crosses calls. Use persisted heap order for successor termination, cursor-owned leases for retained events, an accounted compact timeline plus in-place derived columns, and one SST stats synchronization path.

**Tech Stack:** C++17, GoogleTest, CMake/CTest, Cedar temporal MemTable/SST cursors, `QueryMemoryAccount`.

## Global Constraints

- Strict TDD: observe a behavior-specific RED before production edits and a focused GREEN afterward.
- Do not commit, reset, clean, checkout, revert, or modify unrelated user work.
- Reserve query memory before every new allocation/copy introduced by this plan.
- Preserve cutoff-before-dedupe for every successor candidate later than the selected fact.
- Append RED/GREEN evidence and self-review to `.superpowers/sdd/task-pinned-temporal-scan-report.md`.

---

### Task 1: Ordered successor early termination

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `src/tcypher/executor.cc`
- Modify: `src/tcypher/storage/temporal_scan.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `FindNextPinnedValidBoundary`, global exact-key heap order.
- Produces: `TcypherExecutionStats::boundary_sst_blocks_read` and early-stop behavior.

- [ ] Add a 9000-version single-key SST test that queries the second-newest point and expects `valid_to` at the newest version with one root and one boundary block attempt.
- [ ] Build/run the focused test and record RED from the draining successor implementation or absent stats field.
- [ ] Count boundary block deltas separately and break before popping/advancing a global heap top whose `valid_from <= after_valid_from`.
- [ ] Run successor, session-collision, duplicate, and cancellation tests GREEN.

### Task 2: Cursor-lifetime last-event lease

**Files:**
- Modify: `src/tcypher/storage/temporal_scan.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `ValueRetentionBytes`, `QueryMemoryAccount`.
- Produces: cursor-owned reserve-before-copy retention for `last_physical_event`.

- [ ] Add used-bytes tests that observe retained fixed/payload bytes after a morsel is released, stable replacement charge, EOF cleanup, and destructor cleanup; add a hard-limit copy-failure test.
- [ ] Run the tests and record RED against the uncharged cross-morsel copy.
- [ ] Add reserve/copy/swap/clear helpers to `TemporalScanCursor::Impl`; preserve the old event when new reserve fails and clean retained state on terminal paths.
- [ ] Run memory, duplicate, output-retention, cancellation, and corruption tests GREEN.

### Task 3: Accounted in-place metadata derivation

**Files:**
- Modify: `include/cedar/tcypher/runtime/vector_pipeline.h`
- Modify: `src/tcypher/runtime/vector_pipeline.cc`
- Modify: `src/tcypher/executor.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Changes: `VectorBatchTransform` to `std::function<Status(ColumnBatch*)>`.
- Produces: accounted compact timeline snapshot and retained derived vectors.

- [ ] Add a one-row large-timeline low-hard-limit test that must fail during transform construction before allocation and release to zero; add a full-batch derived-memory hard-limit/lifecycle test.
- [ ] Run focused tests and record RED against the unaccounted map/input copies.
- [ ] Reserve `entries.size() * sizeof(uint64_t)` before compact timeline allocation, validate contiguous commit sequences, and capture a shared lease/vector in the transform.
- [ ] Mutate the scan batch in place, reserve all derived fixed bytes before vector allocation, and attach one shared lease to every derived `FlatVector`.
- [ ] Run metadata, property-offset, predicate, low-memory, and pipeline tests GREEN.

### Task 4: Exact SST attempted-block synchronization

**Files:**
- Modify: `src/columnar/sst_v2.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `StreamingInputCursor::block_attempts()` and `peak_attempted_bytes()`.
- Produces: public `SstV2CursorStats` reflecting every attempted block after Open/Advance.

- [ ] Add a deterministic exact-key empty/false-positive fixture whose public cursor is invalid but whose underlying path attempted at least one indexed block.
- [ ] Run the test and record RED (`blocks_read == 0`).
- [ ] Centralize stats synchronization and call it after Open and every Advance success/failure.
- [ ] Run exact match, empty, recursive/failure, cancellation, and visitor-stat tests GREEN.

### Task 5: Verification and report

**Files:**
- Modify: `.superpowers/sdd/task-pinned-temporal-scan-report.md`

**Interfaces:**
- Consumes: all focused and full verification evidence.
- Produces: final second-review remediation record.

- [ ] Run a broad focused test group covering all four fixes and prior reviewer cases.
- [ ] Run `cmake --build build-v2 -j4 --target test_correctness_kernel && build-v2/tests/test_correctness_kernel` and require zero failures.
- [ ] Run `ctest --test-dir build-v2 --output-on-failure` and require zero failures.
- [ ] Run `git diff --check`, explicit trailing-whitespace scans for untracked v2 files, and searches confirming no temporary mutation remains.
- [ ] Append exact RED/GREEN commands, outputs, changed files, and self-review to the required report.
