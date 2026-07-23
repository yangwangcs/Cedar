# Cedar CBO Executed Choice Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make production point-query access-path decisions and multi-root graph-order decisions observable as both selected and actually executed choices in `EXPLAIN ANALYZE`.

**Architecture:** Extend the existing approved Temporal CBO path rather than adding a parallel planner. `PrepareRootRuntimeFeedback` becomes the single root access-path preparation step: it derives a static `ScanCostEstimate`, chooses base/index/hybrid/intersection before the first execution, optionally applies confident feedback to the same estimate, and stores the selected decision in the query context and stats. The physical runtime records the final executed path after adaptive predicate dropping or advisory fallback, while the multi-root runtime records the graph order when it opens the exact validated plan. The profile serializer publishes one bounded optimizer object.

**Tech Stack:** C++20, Cedar physical planner/runtime, `RuntimeFeedbackStore`, `ExplainAnalyzeRuntimeProfile`, GoogleTest/CTest.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Run every build and test command with `-j1`.
- Keep the internal database format number at `1`.
- Do not add external V2/Vn names, legacy runtime paths, fallback readers, or old-layout compatibility.
- Index results remain advisory candidates and must pass the existing temporal base validation.

---

### Task 1: Define selected and executed optimizer telemetry

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/observability/explain_analyze_profile.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `CandidateSource`, `GraphOrder`, `CostVector` from `cedar/optimizer/cost_model.h`.
- Produces: bounded fields for selected/executed access path, access-path score/cost/rationale/fallback, and selected/executed graph order.

- [x] **Step 1: Write a failing serializer test**

  Extend the physical `EXPLAIN ANALYZE` profile test to require an `optimizer` object with `selected_access_path`, `executed_access_path`, `access_path_score`, all `CostVector` dimensions, `access_path_fallback`, `selected_graph_order`, and `executed_graph_order`. Require `"none"` for shapes without a graph-order decision.

- [x] **Step 2: Run the test to verify RED**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'TcypherExecutorTest.ExplainAnalyzeExecutesTheQueryAndReturnsAProfile' --output-on-failure`

  Expected: FAIL because the serialized profile has no optimizer object.

- [x] **Step 3: Add the telemetry fields and serializer**

  Add explicit presence booleans rather than sentinel enum values. Serialize enum values through bounded switch functions, escape the rationale with the existing JSON helper, and emit every `CostVector` dimension as an integer.

- [x] **Step 4: Run the test to verify GREEN**

  Run the same focused CTest command and expect 1/1 PASS.

### Task 2: Select a static production access path before feedback is confident

**Files:**
- Modify: `src/tcypher/executor.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: pinned `VersionSnapshot`, `IndexCatalogSnapshot`, `PinnedStatsSnapshot`, sidecar/delta candidate estimates, and optional `RuntimeFeedbackStore`.
- Produces: `TcypherExecutionContext::root_access_path` plus selected decision telemetry on every eligible point query, even at zero feedback observations.

- [x] **Step 1: Write failing production tests**

  Add public-database tests proving that the first selective indexed query selects index, a query with incomplete SST coverage selects hybrid, two selective indexed predicates select intersection, and a non-selective indexed query selects base. Assert correctness results and selected decision telemetry.

- [x] **Step 2: Run the four tests to verify RED**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'DurableLogTest.TcypherFirstExecutionChooses|DurableLogTest.TcypherIncompleteCoverageChooses|DurableLogTest.TcypherTwoPredicatesChoose' --output-on-failure`

  Expected: FAIL because `root_access_path` is currently assigned only after confident feedback and incomplete coverage is not part of the preparation estimate.

- [x] **Step 3: Implement one preparation path**

  Refactor `PrepareRootRuntimeFeedback` into root access-path preparation. Compute uncovered rows from relevant pinned SST row counts lacking a usable matching Fragment, unindexed pinned MemTable events, direct committed events, and session-overlay events. Choose the static decision first. If a feedback record has at least the existing confidence threshold, apply it to the same estimate and choose again. Store the selected decision and keep the existing snapshot-scoped feedback key/observation semantics.

- [x] **Step 4: Run the focused tests to verify GREEN**

  Run the same focused command and expect all new cases to pass.

### Task 3: Record the path and graph order actually opened by the runtime

**Files:**
- Modify: `src/tcypher/runtime/query_runtime.cc`
- Modify: `src/tcypher/executor.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: selected `root_access_path`, adaptive candidate state, advisory fallback, and `PhysicalMultiHashJoinPlan::graph_order`.
- Produces: exact executed access path and graph order in `TcypherExecutionStats`.

- [x] **Step 1: Write failing runtime tests**

  Extend the existing non-selective feedback test to require selected/executed base with zero candidate work. Add an adaptive-fallback case requiring selected index/hybrid and executed base with `access_path_fallback=true`. Extend the physical three-root `EXPLAIN ANALYZE` test to require identical selected/executed graph order and the exact parent plan identity.

- [x] **Step 2: Run the tests to verify RED**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'DurableLogTest.TcypherReusesConfidentSnapshotScopedRuntimeFeedback|TcypherExecutorTest.PhysicalThreeRootExplainAnalyzeExecutesSameTypedPlan' --output-on-failure`

  Expected: FAIL because the runtime does not publish final path or graph-order fields.

- [x] **Step 3: Record exact runtime transitions**

  Initialize executed access path from the selected path when the root runtime opens. When adaptive intersection drops to one predicate, downgrade executed intersection to index/hybrid according to the selected coverage class. When `FallBackFromAdvisoryIndex` runs, set executed base and the fallback flag. In `OpenPhysicalMultiHashJoinRuntime`, copy `plan->graph_order` into selected and executed graph-order fields only after plan validation succeeds.

- [x] **Step 4: Run the tests to verify GREEN**

  Run the same focused command and expect all cases to pass.

### Task 4: Close the focused release evidence row

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Create: `results/release-closure-20260723-cbo-executed-choices/README.md`
- Create: `results/release-closure-20260723-cbo-executed-choices/manifest.json`
- Create: `results/release-closure-20260723-cbo-executed-choices/focused-ctest.log`
- Create: `results/release-closure-20260723-cbo-executed-choices/normal-ctest.log`
- Create: `results/release-closure-20260723-cbo-executed-choices/SHA256SUMS`

**Interfaces:**
- Consumes: production-path tests and serialized profiles from Tasks 1-3.
- Produces: a durable artifact mapping base/index/hybrid/intersection and adjacency-first/index-first choices to exact query, snapshot, plan id, selected/executed choice, cost vector, counters, command, host, and source identity.

- [x] **Step 1: Run the focused matrix**

  Run the new CBO/access-path/graph-order tests with `-j1` and archive the complete log.

- [x] **Step 2: Run the normal matrix**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 --output-on-failure`

  Archive the complete log; do not refresh sanitizer matrices until the next larger functional batch.

- [x] **Step 3: Write and validate the artifact**

  Record source commit, dirty entry count, host/toolchain, database format `1`, exact commands, per-case expected and observed choices, plan ids, cost dimensions, correctness row hashes, and file hashes. Validate JSON and SHA-256 from the repository root.

- [x] **Step 4: Update the completion matrix conservatively**

  Mark only the newly proven real access-path/graph-order execution and explain evidence as complete. Preserve repair/drop/reopen, feedback snapshot visibility, Blob/resource, ingestion-pressure, sanitizer, production-scale, and paper gates as open.

## Completion Record

Completed 2026-07-23. The focused matrix passes 60/60 and the fresh normal
matrix passes 855/855 with `-j1`. During normal verification, the root access
decision was found to be incorrectly disabling independent target and
relationship dynamic filters. The final implementation scopes root CBO and its
selected predicate indices to the root binding while preserving non-root
candidate domains. Evidence is archived under
`results/release-closure-20260723-cbo-executed-choices/`.
