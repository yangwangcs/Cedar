# Cedar CBO Next-Pipeline Reoptimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reconsider a HashJoin build side immediately before its pipeline starts, using a bounded prefix of both unopened inputs and switching only when the sample proves one side is no larger.

**Architecture:** An adaptive wrapper samples at most two `ResultBatch` objects from the logical left and right inputs before constructing the existing `PhysicalHashJoinResultStream`. Sampled batches are retained in replay streams and emitted to the join in their original order. The wrapper changes the planned build side only when one input reaches EOF within the budget and its complete row count is no larger than the other input's observed rows, or when both inputs reach EOF and one is strictly smaller; otherwise the static CBO choice remains unchanged.

**Tech Stack:** C++17, `QueryResultStream`, `PhysicalHashJoinResultStream`, `PhysicalHashJoinPlan`, `PhysicalMultiHashJoinPlan`, GoogleTest, EXPLAIN ANALYZE JSON.

## Global Constraints

- Sampling is bounded to two result batches per input per HashJoin pipeline.
- Sampled rows are replayed exactly once in original input order.
- Reoptimization happens before the Join emits any output row.
- QuerySnapshot, child plans, join keys, output mappings, null semantics, and exact comparison remain unchanged.
- An incomplete sample never claims that its side is smaller.
- The static build side remains in force when bounded evidence is inconclusive.
- The same wrapper is used for two-input and every general multi-join step.
- Broad resource normalization, fault injection, and sanitizer matrices remain deferred to the final constraint phase.
- Preserve the dirty worktree; do not reset, clean, commit, or push.

---

### Task 1: Reproduce a wrong static build-side estimate

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `PlanPhysicalHashJoin`, `BuildPhysicalQuerySnapshot`, `OpenPhysicalHashJoinRuntime`.
- Produces: `TcypherExecutorTest.PhysicalHashJoinReoptimizesNextPipelineFromBoundedPrefix`.

- [ ] Add a test that plans `MATCH (a {id: 1}) MATCH (b) WHERE a.name=b.name` with false planning estimates `left=100`, `right=10`, proving the static plan selects the right build side while actual left input has one row and right input has three.
- [ ] Execute the physical plan with `batch_capacity=1` and assert one correct result plus:

```cpp
EXPECT_EQ(stats->pipeline_reoptimization_checks, 1U);
EXPECT_EQ(stats->pipeline_reoptimizations, 1U);
EXPECT_EQ(stats->pipeline_reoptimization_sampled_rows, 3U);
EXPECT_EQ(stats->hash_join_build_side_switches, 1U);
EXPECT_EQ(stats->hash_join_build_side_left, 1U);
```

- [ ] Build and run the focused test. Expected RED: the new counters do not exist and the runtime still uses the statically planned right build side.

### Task 2: Add bounded prefix replay and runtime choice

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/tcypher/executor.cc`
- Modify: `src/observability/explain_analyze_profile.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`

**Interfaces:**
- Produces: `PrefixReplayResultStream`, `AdaptivePhysicalHashJoinResultStream`.
- Consumes: the existing `PhysicalHashJoinResultStream` without changing its exact join implementation.

- [ ] Add execution/profile counters:

```cpp
uint64_t pipeline_reoptimization_checks = 0;
uint64_t pipeline_reoptimizations = 0;
uint64_t pipeline_reoptimization_sampled_rows = 0;
uint64_t pipeline_reoptimization_sampled_batches = 0;
uint64_t hash_join_build_side_switches = 0;
```

- [ ] Implement `PrefixReplayResultStream` with a deque of sampled batches followed by the original child stream. `Next` moves each prefix batch exactly once before delegating; `terminal_status` and resource stats delegate to the child.
- [ ] Implement `AdaptivePhysicalHashJoinResultStream` with `kSampleBatchLimit = 2`. Its first `Next` samples both logical inputs, records observed rows/batches, chooses the actual build side using only the proof rules in Global Constraints, updates counters, wraps both inputs in replay streams, and creates the existing `PhysicalHashJoinResultStream` delegate.
- [ ] Pass logical left/right inputs, both key-column vectors, the planned build side, and existing join constructor state into the wrapper.
- [ ] Update `hash_join_build_side_left` after the runtime decision so it reports the executed side.
- [ ] Run the Task 1 test. Expected GREEN with one switch and sampled rows `3` (`left=1`, first two right batches=`2`).

### Task 3: Preserve the static plan when evidence is incomplete

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: the adaptive wrapper and reoptimization counters.
- Produces: `TcypherExecutorTest.PhysicalHashJoinKeepsPlannedSideWhenPrefixIsInconclusive`.

- [ ] Add ten left/right rows, `batch_capacity=1`, and equal join-key groups so neither input reaches EOF within two sampled batches.
- [ ] Assert correct output, `pipeline_reoptimization_checks == 1`, `pipeline_reoptimizations == 0`, `hash_join_build_side_switches == 0`, and the executed side equals the static plan.
- [ ] Run both tests and the existing join-order/spill/null/composite-key tests.

### Task 4: Multi-join and observability integration

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: the wrapper from Task 2 in `OpenPhysicalMultiHashJoinRuntime`.
- Produces: multi-step checks and EXPLAIN ANALYZE fields.

- [ ] Replace direct `PhysicalHashJoinResultStream` construction in both two-input and multi-join open paths with the adaptive wrapper.
- [ ] Extend the existing three-root dynamic-filter test to assert `pipeline_reoptimization_checks == 2`, one per join pipeline.
- [ ] Serialize reoptimization checks, switches, sampled rows, and sampled batches in the scheduler/join profile objects and assert nonzero bounded sample values under EXPLAIN ANALYZE.
- [ ] Run focused HashJoin/multi-join/spill/CBO/Explain tests, normal CTest, `git diff --check`, and an explicit trailing-whitespace scan.
- [ ] Record exact verification counts and deferred resource/sanitizer work in the progress ledger without committing.
