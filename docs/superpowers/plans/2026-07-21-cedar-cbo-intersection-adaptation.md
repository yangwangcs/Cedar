# Cedar CBO Intersection Adaptation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adapt a multi-predicate advisory intersection into the remaining selective index path when one completed index stream becomes non-selective, while retaining ordinary property gather and final predicate filtering for correctness.

**Architecture:** Keep the physical plan and all predicates unchanged. The root runtime tracks candidate samples per predicate while lazy SST sidecars and MemTable delta indexes complete. A predicate whose sample reaches the existing 500-per-mille threshold is removed only from candidate generation when another candidate predicate remains; unopened sources used exclusively by the removed predicate are skipped. The ordinary PropertyGather and Filter operators continue evaluating every predicate, so this optimization cannot alter temporal visibility or results. If the last active candidate predicate becomes non-selective with unopened sources remaining, the existing full base fallback is used.

**Tech Stack:** C++17, immutable SST index sidecars, version-chain `TemporalMemTable`, physical root runtime, GoogleTest.

## Global Constraints

- The pinned `QuerySnapshot`, temporal scan sources, and physical predicate list never change.
- Candidate adaptation may remove a predicate only from advisory candidate generation, never from final filtering.
- Candidate intersection remains entity-ID based; edge identity adaptation is outside this root-vertex slice.
- The existing deterministic threshold remains 500 per mille of `runtime_feedback_base_rows`.
- SST and MemTable sources are sampled only after successful source materialization.
- Broad lifetime, fault, crash, oracle, ASAN, UBSAN, and TSAN matrices remain deferred to the unified final phase.
- This dirty worktree must not be reset, cleaned, committed, or pushed.

---

### Task 1: Lazy SST intersection RED test

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `OpenPhysicalRootForTest`, lazy `TcypherIndexSource`, `runtime_feedback_base_rows`.
- Produces: observable behavior through `index_adaptive_intersection_predicates_dropped` and existing materialized/skipped counters.

- [x] **Step 1: Write the failing test**

  Build two equality definitions for `name` and `city`. Put six matching `name` rows across three lazy fragments so the first fragment reaches the threshold, and put one matching `city` row in a fourth fragment. Execute `WHERE n.name = 'Ada' AND n.city = 'Paris'` and assert entity `1` is returned, only the first `name` fragment plus the `city` fragment are materialized, two unopened `name` fragments are skipped, and one intersection predicate is dropped.

- [x] **Step 2: Run the focused test to verify RED**

  Run: `build/tests/test_correctness_kernel --gtest_filter='DurableLogTest.AdvisoryIntersectionDropsNonSelectiveLazySstPredicate'`

  Expected: FAIL because the current runtime materializes all four fragments and has no dropped-predicate counter.

### Task 2: Active candidate predicate state and SST adaptation

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `src/tcypher/runtime/query_runtime.cc`

**Interfaces:**
- Produces: `active_candidate_predicate_indices`, per-plan-predicate adaptive samples, and `index_adaptive_intersection_predicates_dropped`.
- Preserves: `plan->predicates()` as the final PropertyGather/Filter contract.

- [x] **Step 1: Add minimal active-predicate state**

  Initialize all selected predicate indices as active after metadata coverage validation. Make candidate preparation iterate that active index vector while looking up the original physical predicate, canonical predicate, and selected definition by plan index.

- [x] **Step 2: Add completed-SST sampling**

  After a lazy sidecar is verified, count matching postings for every active predicate using that source's definition. Saturating-add the count into its per-predicate sample and the existing aggregate sampled-candidate counter.

- [x] **Step 3: Drop or fall back**

  At the 50% threshold, remove the predicate from active candidate generation when at least one other active predicate remains. Increment the reoptimization and dropped-predicate counters. Skip later lazy SST sources whose definition is no longer used by any active predicate. If this is the last predicate and matching unopened sources remain, call `FallBackFromAdvisoryIndex()`.

- [x] **Step 4: Run the focused test to verify GREEN**

  Run the Task 1 filter and expect PASS.

### Task 3: MemTable intersection adaptation

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`

**Interfaces:**
- Consumes: completed `TemporalMemTableCursor` source events and `MemtableDeltaIndex`.
- Produces: the same active-predicate decision and skipped-delta-source behavior as the SST path.

- [x] **Step 1: Write the failing MemTable test**

  Build two pinned MemTables and complete delta descriptors for both `name` and `city`. Make the first `name` source cross the threshold, assert the second `name` source is skipped, both `city` sources remain available, entity `1` is returned, and query memory returns to zero.

- [x] **Step 2: Run the focused test to verify RED**

  Run: `build/tests/test_correctness_kernel --gtest_filter='TcypherExecutorTest.AdvisoryIntersectionDropsNonSelectiveDeltaPredicate'`

  Expected: FAIL because every delta descriptor is currently built.

- [x] **Step 3: Reuse completed-source sampling for delta indexes**

  Count unique matching entity IDs per completed delta source for every active predicate using that source definition. Apply the same drop/last-predicate fallback rule and bypass unopened delta descriptors unused by active predicates.

- [x] **Step 4: Run both intersection tests to verify GREEN**

  Run both new filters and expect PASS.

### Task 4: Profile and focused regression

**Files:**
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/observability/explain_analyze_profile.cc`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Produces: bounded JSON field `adaptive_intersection_predicates_dropped`.

- [x] **Step 1: Export the dropped-predicate counter**

  Copy the execution counter into `IndexRuntimeProfile` and emit it beside the other adaptive counters.

- [x] **Step 2: Run focused regression**

  Run the two new tests plus existing advisory adaptation, candidate intersection, cancellation, memory cleanup, Explain Analyze, and CBO tests.

- [x] **Step 3: Run normal CTest and hygiene checks**

  Run normal CTest, `git diff --check`, and the explicit trailing-whitespace scan. Do not start the final fault/sanitizer matrix.

- [x] **Step 4: Update progress**

  Record the single-index candidate downgrade behavior and leave dynamic downstream filters, next-pipeline reoptimization, and path-frontier repartitioning as later functionality.
