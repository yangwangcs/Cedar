# Cedar CBO Runtime Feedback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded, snapshot-scoped runtime feedback that classifies predicate selectivity and adjusts the next legal base/index/hybrid/intersection cost decision without changing temporal visibility.

**Architecture:** A thread-safe optimizer-owned `RuntimeFeedbackStore` retains a bounded LRU set of aggregated observations keyed by normalized predicate shape, schema epochs, catalog generation, statistics snapshot, and selectivity bucket. The cost model consumes only confident matching feedback and only adjusts cardinality estimates; existing catalog legality and base-validation rules remain authoritative. Query execution records candidates, survivors, pages, interval splits, and Blob reads after terminal completion through a shared store owned by `CedarDatabaseV2`.

**Tech Stack:** C++17, Cedar typed optimizer/runtime contracts, GoogleTest, CMake.

## Global Constraints

- Feedback must never make an unavailable or incomplete index path legal.
- Feedback keys must include catalog and statistics generations and must not cross schema epochs.
- Storage is bounded and deterministic; least-recently-used entries are evicted at capacity.
- A single observation is diagnostic only; cost correction requires at least two observations.
- Existing dirty worktree changes are preserved and no commit is created during this unified implementation pass.

---

### Task 1: Bounded feedback store

**Files:**
- Create: `include/cedar/optimizer/runtime_feedback.h`
- Create: `src/optimizer/runtime_feedback.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `SelectivityBucket ClassifySelectivity(uint64_t candidates, uint64_t base_rows)`.
- Produces: `RuntimeFeedbackStore::Observe`, `Lookup`, `ApplyToEstimate`, and `size`.
- Consumes: `ScanCostEstimate` from `cedar/optimizer/cost_model.h`.

- [x] Add `RuntimeFeedbackStoreTest` cases proving exact-key isolation, two-sample confidence, saturated aggregation, and deterministic LRU eviction.
- [x] Build and run `RuntimeFeedbackStoreTest.*`; expect compile failure because the header and types do not exist.
- [x] Implement the bounded mutex-protected store. `ApplyToEstimate` replaces candidate cardinality with the rounded observed mean only when `observations >= 2`, clamps it to `base_rows`, and leaves coverage/legality fields unchanged.
- [x] Re-run `RuntimeFeedbackStoreTest.*`; expect all cases to pass.

### Task 2: Cost model feedback consumption

**Files:**
- Modify: `include/cedar/optimizer/cost_model.h`
- Modify: `src/optimizer/cost_model.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: a corrected `ScanCostEstimate` returned by `RuntimeFeedbackStore::ApplyToEstimate`.
- Produces: unchanged `ChooseAccessPathDecision` legality with a rationale suffix identifying confident runtime feedback.

- [x] Add a failing cost-model test where stale statistics choose base scan, two matching observations correct candidate cardinality, and the legal index path wins; assert a mismatched catalog generation has no effect.
- [x] Run `CostModelTest.*`; expect the new feedback-selection assertion to fail.
- [x] Add an optional `feedback_applied` flag to `ScanCostEstimate` and include `"runtime feedback"` in the selected decision rationale without changing source availability rules.
- [x] Re-run `CostModelTest.*`; expect all cases to pass.

### Task 3: Database-owned feedback lifetime and execution recording

**Files:**
- Modify: `include/cedar/db/cedar_database_v2.h`
- Modify: `src/db/cedar_database_v2.cc`
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `src/tcypher/executor.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `TcypherExecutionContext::runtime_feedback` as `std::shared_ptr<RuntimeFeedbackStore>`.
- Produces: pending feedback identity and planned base/candidate counts in `TcypherExecutionStats`.
- Consumes: final operator counters from `OperatorRuntimeStatsRegistry` after the query stream reaches terminal status.

- [x] Add a failing database test that runs the same indexed predicate twice, verifies the first run records diagnostic feedback, and verifies the third planning pass reports confident feedback use while returning the same rows as a base scan.
- [x] Run the focused database/index tests; expect no feedback record or planning-use counter.
- [x] Own one shared feedback store in `CedarDatabaseV2`, attach it to every T-Cypher execution context, derive a normalized predicate key without literal bytes, and record final candidate/survivor/page/Blob observations exactly once at terminal completion.
- [x] Re-run the focused tests; expect identical result sets and confident feedback use only on the third execution.

### Task 4: Explain and regression verification

**Files:**
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/observability/explain_analyze_profile.cc`
- Modify: `.superpowers/sdd/progress.md`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: profile fields `runtime_feedback_bucket`, `runtime_feedback_observations`, and `runtime_feedback_applied`.

- [x] Add failing `EXPLAIN ANALYZE` assertions for feedback identity, confidence, and application state.
- [x] Serialize the three bounded fields without embedding literal values or unbounded query text.
- [x] Run focused index/CBO/profile tests, normal CTest, focused TSAN, and `git diff --check`.
- [x] Record verified counts and any still-deferred adaptive mid-query actions in `.superpowers/sdd/progress.md`.
