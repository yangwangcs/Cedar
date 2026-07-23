# Cedar CBO MemTable Delta Adaptation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop building unopened MemTable delta-index sources when completed early sources already show non-selective candidate density, then preserve correctness through the pinned base MemTable scan.

**Architecture:** Evaluate density only after one `TemporalMemTableCursor` reaches a clean terminal state. Count unique matching entity IDs from the fully captured source events using the canonical physical predicate. Accumulate with prior SST samples, compare against the existing 500-per-mille base-row threshold, and invoke the shared advisory cleanup/fallback path before any candidate set is installed.

**Tech Stack:** C++17, version-chain `TemporalMemTable`, `MemtableDeltaIndex`, GoogleTest.

## Global Constraints

- Frozen and active MemTable snapshot pins remain owned by `memtable_event_sources` during fallback.
- Partially built delta sources are never used for candidate restriction.
- Candidate counting applies only to one predicate and deduplicates entity IDs within each completed source.
- Cancellation and memory leases retain their existing cleanup behavior.
- Full fault and sanitizer matrices remain deferred to the unified final stage.

---

### Task 1: Multi-MemTable RED test

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

- [x] Build two pinned `TemporalMemTable` sources containing six matching vertices, four in the first source and two in the second.
- [x] Configure matching lazy delta descriptors with one pinned generation and a six-row base estimate.
- [x] Assert the query returns all six rows, materializes one delta source, skips one unopened source, and releases query memory.
- [x] Run the focused test and observe missing counters/continued second-source materialization.

### Task 2: Completed-source density switch

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `src/tcypher/runtime/query_runtime.cc`

- [x] Add a skipped-delta-source counter.
- [x] Count unique matching entities after a delta cursor finishes successfully.
- [x] Accumulate with the existing adaptive candidate sample and evaluate the same overflow-safe 50% threshold.
- [x] Release built delta indexes and leases through `FallBackFromAdvisoryIndex` before opening the base scan.
- [x] Run delta cursor, cancellation, memory admission, and result-equivalence tests.

### Task 3: Profile and regression

**Files:**
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/observability/explain_analyze_profile.cc`
- Modify: `.superpowers/sdd/progress.md`

- [x] Export skipped unopened delta sources in the bounded index profile.
- [x] Run focused tests, CTest, focused TSAN, diff checks, and whitespace checks.
- [x] Keep multi-predicate, dynamic downstream filter, frontier repartitioning, and next-pipeline replanning listed as remaining functionality.
