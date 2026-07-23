# Cedar CBO Mid-Query Adaptation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop opening remaining advisory index fragments when early candidate density proves the index path non-selective, then execute the untouched full base scan with identical results.

**Architecture:** Reuse the existing per-fragment yield boundary in `QueryRuntimeState::AdvanceIndexPreparationQuantum`. After a lazy SST sidecar is validated and materialized, count postings matching the already-canonicalized single predicate. When cumulative matches reach 50% of the pinned base-row estimate and unopened fragments remain, release every advisory allocation, mark an adaptive reoptimization, and open the ordinary base temporal scan. Partial candidate state is never installed into `TemporalScanSpec`.

**Tech Stack:** C++17, Cedar physical runtime, immutable IndexSidecar, GoogleTest.

## Global Constraints

- Adaptation is advisory only and must preserve the same QuerySnapshot and base validation semantics.
- The first slice applies only to one physical predicate; multi-predicate intersection remains unchanged.
- The threshold is deterministic at 500 per mille and requires at least one unopened lazy SST fragment.
- Switching discards all partial index state before opening the base scan.
- No final fault/sanitizer matrix is run until the unified verification phase.

---

### Task 1: Candidate-density RED test

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

- [x] Create three valid lazy SST/index fragment pairs where the first fragment already matches four of six base entities.
- [x] Execute an equality query and assert all six matching rows are returned after adaptation.
- [x] Assert exactly one SST index source was materialized, two unopened sources were skipped, and one adaptive reoptimization was recorded.
- [x] Run the focused test and observe the missing counters/continued materialization failure.

### Task 2: Fragment-boundary adaptation

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `src/tcypher/runtime/query_runtime.cc`

- [x] Add bounded counters for adaptive reoptimizations, sampled candidates, and skipped unopened fragments.
- [x] Count matching postings only after sidecar validation and only for a single canonical predicate.
- [x] Compare cumulative matches against `runtime_feedback_base_rows` using overflow-safe integer arithmetic.
- [x] On threshold crossing, call the existing full advisory cleanup/fallback path before candidate installation.
- [x] Re-run focused index preparation, memory cleanup, cancellation, and result-equivalence tests.

### Task 3: Profile and verification

**Files:**
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/observability/explain_analyze_profile.cc`
- Modify: `.superpowers/sdd/progress.md`

- [x] Expose adaptive reoptimization and skipped-fragment counters in the bounded index profile object.
- [x] Run focused CBO/index/Explain tests, normal CTest, focused TSAN, `git diff --check`, and trailing-whitespace checks.
- [x] Record that dynamic join filters, path-frontier repartitioning, and next-pipeline replanning remain later slices.
