# Cedar T-Cypher Disconnected Cross Join Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute disconnected point-in-time multi-root T-Cypher queries through explicit bounded, spillable, cancellable physical cross-join steps with no legacy full materialization.

**Architecture:** Keep the existing `PhysicalMultiHashJoinPlan` as the multi-root composition boundary, but make each step's explicit `PhysicalOperatorKind` authoritative: keyed steps are `kHashJoin` and disconnected attachments are `kCrossJoin`. The runtime dispatches cross steps to a dedicated adaptive cross-join stream that retains only one replay side, spills that side at the query soft limit, and streams the Cartesian output in bounded batches.

**Tech Stack:** C++17, Cedar physical/logical T-Cypher planner, vectorized `QueryResultStream`, `QueryMemoryAccount`, `PartitionedSpillSet`, GoogleTest, CMake, ASAN/UBSAN/TSAN.

## Global Constraints

- Preserve every existing worktree modification; do not reset, clean, or revert unrelated files.
- Do not stage, commit, or push.
- Production behavior follows RED-GREEN TDD; observe the expected failing assertion before implementation.
- Random coverage uses fixed `std::mt19937_64` seeds and reports seed plus case on failure.
- A cross join is represented only by `PhysicalOperatorKind::kCrossJoin`, never by `kHashJoin` with empty keys.
- Do not reuse `CrossJoinRootResultStream` or `ExecuteMultiRootMatchAsOf` as the physical runtime.
- Retain LogicalKey, TemporalEvent, version-chain MemTable, SST v2, and bitemporal semantics unchanged.
- Build the large correctness target with `-j1` to avoid the previously observed compiler OOM.

---

### Task 1: Admit Disconnected Graphs and Produce Explicit Cross Steps

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `include/cedar/tcypher/physical_plan.h`
- Modify: `src/tcypher/physical_plan.cc`

**Interfaces:**
- Consumes: `BoundTcypherStatement`, `LogicalPlan::join_edges`, `PhysicalCardinalityEstimate`.
- Produces: `PhysicalOperatorKind::kCrossJoin`; mixed `PhysicalMultiHashJoinStep` sequences whose key vectors are empty only for cross steps.

- [ ] **Step 1: Convert the existing fallback execution test to a failing physical-plan expectation**

Rename `DisconnectedThreeRootQueryKeepsLegacyMaterializationFallback` to
`DisconnectedThreeRootQueryUsesPhysicalCrossJoin` and assert:

```cpp
EXPECT_EQ(rows, 8U);
EXPECT_EQ(stats->physical_multi_join_builds, 1U);
EXPECT_GT(stats->pipeline_builds, 0U);
EXPECT_EQ(stats->legacy_multi_root_materialized_rows, 0U);
EXPECT_EQ(stats->last_physical_plan_id, stats->executed_physical_plan_id);
```

- [ ] **Step 2: Add a failing mixed-step planner test**

Bind and lower:

```cpp
"FOR VALID_TIME AS OF 10 MATCH (a) MATCH (b) MATCH (c) "
"WHERE a.name = b.name RETURN a, b, c;"
```

Require `CanPlanPhysicalMultiHashJoin(...) == true`, a successful plan, two
steps, exactly one `kHashJoin`, exactly one `kCrossJoin`, non-empty keys for the
hash step, and empty keys for the cross step.

- [ ] **Step 3: Run the two tests and record RED**

Run:

```bash
cmake --build build-v2 --target test_correctness_kernel -j1
build-v2/tests/test_correctness_kernel \
  --gtest_filter='TcypherExecutorTest.DisconnectedThreeRootQueryUsesPhysicalCrossJoin:TcypherPhysicalPlanTest.DisconnectedComponentBuildsExplicitCrossJoinStep'
```

Expected: the execution test reports `physical_multi_join_builds == 0`; the
planner test reports the disconnected statement is not a physical candidate.

- [ ] **Step 4: Add the physical operator kind and name**

In `PhysicalOperatorKind`, add `kCrossJoin` immediately after `kHashJoin`. In
`PhysicalOperatorKindName`, return `"CrossJoin"` for it.

- [ ] **Step 5: Make connectivity a step property, not a candidate property**

In `CanPlanPhysicalMultiHashJoin`:

- allow an empty `statement.joins` collection;
- keep all root ownership, point-scope, projection, local-predicate, and fixed
  expansion checks;
- retain the two-root connected relationship-free query on the existing
  `PhysicalHashJoinPlan` path, but admit a two-root query when it has no join;
- remove the final connected-graph traversal requirement.

In `PlanPhysicalMultiHashJoin`, remove `logical_plan.join_edges.empty()` from
the rejection condition.

- [ ] **Step 6: Extend deterministic join-order selection**

For the dynamic-programming entry add a cross-step count:

```cpp
struct DpEntry {
  bool populated = false;
  uint64_t cumulative_cost = 0;
  uint64_t accumulated_rows = 0;
  uint32_t cross_steps = 0;
  std::vector<uint32_t> order;
};
```

Permit every unattached candidate. Determine `connected` from adjacency; add
one to `cross_steps` when false. Compare entries by cumulative cost, then cross
step count, then lexicographic binding-sorted order. For the greedy path, choose
the smallest estimated connected candidate when one exists; otherwise choose
the smallest estimated unattached root as the next component.

- [ ] **Step 7: Emit the authoritative step kind**

After collecting equality key pairs for a step:

```cpp
step.join.kind = step.accumulated_key_columns.empty()
    ? PhysicalOperatorKind::kCrossJoin
    : PhysicalOperatorKind::kHashJoin;
```

Require the two key vectors to be both empty or both non-empty. Preserve the
existing output-layout pruning and saturated estimate propagation for either
kind.

- [ ] **Step 8: Run planner tests**

Run the Task 1 filter again. Expected: planner test passes; execution test now
reaches physical-plan validation/runtime and may still fail until Tasks 2-3.

---

### Task 2: Validate and Explain Mixed Hash/Cross Plans

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `src/tcypher/physical_plan.cc`

**Interfaces:**
- Consumes: mixed `PhysicalMultiHashJoinPlan` steps from Task 1.
- Produces: strict structural validation and EXPLAIN text containing `CrossJoin`.

- [ ] **Step 1: Add failing validator mutations**

Starting from the mixed plan, create copies that:

```cpp
// Invalid: hash join without keys.
invalid_hash.steps[hash_index].accumulated_key_columns.clear();
invalid_hash.steps[hash_index].input_key_columns.clear();

// Invalid: cross join with keys.
invalid_cross.steps[cross_index].accumulated_key_columns = {0};
invalid_cross.steps[cross_index].input_key_columns = {0};

// Invalid: unknown operator.
invalid_kind.steps[cross_index].join.kind =
    static_cast<PhysicalOperatorKind>(255);
```

Require all three plans to return `InvalidArgument`.

- [ ] **Step 2: Run validator test and observe RED**

Run:

```bash
build-v2/tests/test_correctness_kernel \
  --gtest_filter=TcypherPhysicalPlanTest.MultiJoinValidatorDistinguishesHashAndCrossSteps
```

Expected: current validator rejects every cross step or fails to distinguish
keyed/empty step invariants.

- [ ] **Step 3: Update `ValidatePhysicalMultiHashJoinPlan`**

Accept only `kHashJoin` and `kCrossJoin`. Define:

```cpp
const bool hash_step = step.join.kind == PhysicalOperatorKind::kHashJoin;
const bool cross_step = step.join.kind == PhysicalOperatorKind::kCrossJoin;
const bool keys_match = step.accumulated_key_columns.size() ==
    step.input_key_columns.size();
const bool keys_valid = hash_step
    ? keys_match && !step.accumulated_key_columns.empty()
    : keys_match && step.accumulated_key_columns.empty();
```

Run type/ownership validation over key pairs only for a hash step. Keep all
output, estimate, build-side, operator-id, and pipeline checks common.

- [ ] **Step 4: Verify EXPLAIN formatting**

Add a planner assertion:

```cpp
const std::string explain = FormatPhysicalMultiHashJoinPlan(*plan);
EXPECT_NE(explain.find("CrossJoin"), std::string::npos) << explain;
EXPECT_NE(explain.find("HashJoin"), std::string::npos) << explain;
```

- [ ] **Step 5: Run planner/validator tests**

Expected: all Task 1-2 physical-plan tests pass.

---

### Task 3: Implement the Bounded In-Memory Cross-Join Runtime

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`

**Interfaces:**
- Consumes: two child `QueryResultStream`s, output mappings, planned build side,
  `batch_capacity`, cancellation, memory account, and execution stats.
- Produces: `AdaptivePhysicalCrossJoinResultStream` and
  `PhysicalCrossJoinResultStream`, both internal to `query_runtime.cc`.

- [ ] **Step 1: Add failing physical execution tests**

Cover:

- the partially disconnected three-root query returns eight rows;
- `physical_multi_join_builds == 1`;
- `pipeline_builds == 2` for its hash and cross steps;
- `legacy_multi_root_materialized_rows == 0`;
- a fully disconnected three-root query returns eight rows through two cross
  steps;
- an empty child produces zero rows and preserves the parent plan identity.

- [ ] **Step 2: Run execution tests and observe RED**

Expected: `OpenPhysicalMultiHashJoinRuntime` constructs an adaptive hash join
with empty key vectors or rejects the cross step.

- [ ] **Step 3: Implement retained-row accounting helpers**

Reuse the existing `ResultValueCell`, `CellAt`, `CellPayloadBytes`,
`AddSingleCell`, and `StoredRow` conventions. Charge each retained row as:

```cpp
uint64_t bytes = sizeof(StoredRow) +
    row.capacity() * sizeof(ResultValueCell);
for (const ResultValueCell& cell : row) {
  bytes += CellPayloadBytes(cell);
}
```

Reserve before retaining; release the exact total in the destructor and on the
transition to spill.

- [ ] **Step 4: Implement `PhysicalCrossJoinResultStream` in-memory mode**

Constructor inputs mirror the adaptive hash join except there are no key
vectors. Drain the selected replay/build stream once into accounted
`StoredRow`s. Keep one streaming/probe `ResultBatch` and row cursor. For each
probe row, emit every retained build row according to
`PhysicalHashJoinPlan::Output`. Stop at `batch_capacity`, retaining cursors for
the next `Next()` call.

Reject an output mapping beyond its source width and reject a missing value for
a non-nullable output. Preserve scalar and structured cells through
`AddSingleCell`.

- [ ] **Step 5: Implement adaptive side selection**

Add `AdaptivePhysicalCrossJoinResultStream` with the same two-batch bounded
sampling contract as `AdaptivePhysicalHashJoinResultStream`. Select the replay
side by sampled exhaustion/rows, falling back to `step.build_side`. Replay
sampled prefixes through `PrefixReplayResultStream` and instantiate the core
stream with a `probe_is_left` mapping flag.

- [ ] **Step 6: Dispatch by explicit operator kind**

In `OpenPhysicalMultiHashJoinRuntime`:

```cpp
if (step.join.kind == PhysicalOperatorKind::kHashJoin) {
  accumulator = std::make_unique<AdaptivePhysicalHashJoinResultStream>(...);
} else if (step.join.kind == PhysicalOperatorKind::kCrossJoin) {
  accumulator = std::make_unique<AdaptivePhysicalCrossJoinResultStream>(...);
} else {
  return Status::Corruption("physical multi join", "unknown join step");
}
```

Increment `pipeline_builds` once after either successful construction.

- [ ] **Step 7: Run execution GREEN tests**

Expected: partial and fully disconnected queries pass with zero legacy
materialization and bounded batch sizes.

---

### Task 4: Spill, Cancellation, and Production Statistics

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `src/tcypher/runtime/query_runtime.cc`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `QueryMemoryAccount`, `PartitionedSpillSet`, cancellation and
  governor extensions.
- Produces: replay-side spill and cross-join production counters.

- [ ] **Step 1: Add failing spill and cancellation tests**

Use a dedicated temporary spill directory and a memory account with a soft
limit small enough to force replay spill. Assert exact row multiset, cleanup of
temporary files, `memory->used_bytes() == 0` after stream destruction, and
positive cross-join spill counters.

Add cancellation tests for cancellation while draining replay input and while
replaying spill. Require terminal `QueryCancelled`, zero leaked memory, and no
remaining spill files.

Add a hard-limit test requiring `QueryMemoryLimit` when even one decoded spill
batch cannot fit.

- [ ] **Step 2: Observe RED for spill/statistics behavior**

Run only the new spill, cancellation, and hard-limit tests. Expected: the
in-memory implementation reaches the soft/hard limit without a spill path or
lacks the required counters.

- [ ] **Step 3: Add execution statistics fields**

Add to `TcypherExecutionStats`:

```cpp
uint64_t physical_cross_join_builds = 0;
uint64_t cross_join_replay_input_rows = 0;
uint64_t cross_join_stream_input_rows = 0;
uint64_t cross_join_output_rows = 0;
uint64_t cross_join_spill_starts = 0;
uint64_t cross_join_spill_bytes = 0;
uint64_t cross_join_build_side_switches = 0;
```

- [ ] **Step 4: Spill the replay side at the soft limit**

Create a one-partition `PartitionedSpillSet` with cancellation, governor,
memory-account, and a write observer that updates `cross_join_spill_bytes`.
When retention would cross the soft limit:

1. open the spill set;
2. append all retained rows as bounded one-row `ResultBatch` records;
3. release retained-row reservations and clear the vector;
4. append subsequent replay rows directly;
5. seal partition zero after replay input ends.

For each streaming row, rewind partition zero and emit all spilled replay rows.
Never retain a second complete input or Cartesian output.

- [ ] **Step 5: Add cancellation and terminal-status checks**

Check cancellation during sample, replay drain, spill append, spill rewind,
spill read, stream input, and output generation. Cache the first terminal error
and return it consistently from `terminal_status()`.

- [ ] **Step 6: Run spill/cancel/hard-limit GREEN tests**

Expected: exact results, positive spill counters, terminal cancellation or
memory-limit status as requested, and zero resource leaks.

---

### Task 5: EXPLAIN ANALYZE and Fixed-Seed Oracle Closure

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify only if a failing test proves necessary: `src/tcypher/executor.cc`
- Modify only if a failing test proves necessary: `src/observability/operator_runtime_stats.cc`
- Modify only if a failing test proves necessary: `include/cedar/observability/operator_runtime_stats.h`

**Interfaces:**
- Consumes: validated mixed plan and physical cross runtime.
- Produces: stable EXPLAIN/ANALYZE evidence and deterministic result equivalence.

- [ ] **Step 1: Add EXPLAIN and EXPLAIN ANALYZE tests**

Require both outputs to contain `PhysicalMultiHashJoinPlan` and `CrossJoin`.
EXPLAIN must not execute. EXPLAIN ANALYZE must execute the same plan identity,
return the expected profile, increment cross-join counters, and keep
`legacy_multi_root_materialized_rows == 0`.

- [ ] **Step 2: Add a fixed-seed oracle test**

Use:

```cpp
constexpr uint64_t kSeed = 0xceda7c2055ULL;
std::mt19937_64 random(kSeed);
```

Generate at least 32 small cases with one to four visible vertices, two to four
roots, and a deterministic mixture of equality edges and disconnected
components. Compute the reference Cartesian product and filter equality edges
in the test. Compare sorted structured output rows. Every assertion message
includes `seed=` and `case=`.

- [ ] **Step 3: Run tests and observe any remaining RED**

Only change EXPLAIN/profile production code when the new test demonstrates a
missing production path. Do not add test-only bypasses.

- [ ] **Step 4: Run the complete T-Cypher multi-root regression set**

Run:

```bash
build-v2/tests/test_correctness_kernel \
  --gtest_filter='TcypherExecutorTest.*ThreeRoot*:TcypherExecutorTest.*MultiJoin*:TcypherExecutorTest.*CrossJoin*:TcypherExecutorTest.PhysicalRelationship*:TcypherPhysicalPlanTest.*MultiJoin*:TcypherPhysicalPlanTest.*CrossJoin*'
```

Expected: all selected tests pass.

---

### Task 6: Stage Verification and Evidence

**Files:**
- Create: `docs/superpowers/plans/2026-07-22-cedar-tcypher-cross-join-evidence.md`
- Modify only for defects proven by verification: implementation/test files
  from Tasks 1-5.

**Interfaces:**
- Consumes: completed cross-join implementation and test suite.
- Produces: reproducible release evidence for this T-Cypher stage.

- [ ] **Step 1: Run the ordinary full suite**

```bash
cmake --build build-v2 --target test_correctness_kernel -j1
ctest --test-dir build-v2 --output-on-failure -j1
```

- [ ] **Step 2: Run sanitizer suites**

Build each correctness target with `-j1`, then run:

```bash
ctest --test-dir build-asan --output-on-failure -j1
ctest --test-dir build-ubsan --output-on-failure -j1
ctest --test-dir build-tsan --output-on-failure -j1
```

- [ ] **Step 3: Run repository integrity checks**

```bash
git diff --check
rg -n "DisconnectedThreeRootQueryKeepsLegacy|UnsupportedConnectedThreeRootSyntaxKeepsLegacy" \
  tests/test_correctness_kernel.cc
```

Expected: `git diff --check` succeeds and neither legacy test name remains.

- [ ] **Step 4: Write the evidence document**

Record exact commands, test totals, sanitizer results, fixed oracle seed,
cross-join spill test configuration, and the production counters proving zero
legacy materialization. Do not claim the broader six-design Goal complete; move
next to the remaining T-Cypher release audit.

## Plan Self-Review

- Every design requirement maps to a task: explicit operator (Tasks 1-2),
  disconnected planning (Task 1), bounded runtime (Task 3), spill/cancellation
  (Task 4), EXPLAIN ANALYZE and oracle (Task 5), sanitizer evidence (Task 6).
- All new behavior begins with a named failing test and an expected RED reason.
- Step kinds, statistics names, runtime class names, and test commands are
  consistent across tasks.
- The plan contains no commit step because the user explicitly prohibited
  staging, committing, and pushing.
