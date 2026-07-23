# Cedar T-Cypher Mixed-Path Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development or superpowers:executing-plans to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking.

**Goal:** Execute finite mixed fixed/variable relationship chains through the
single typed T-Cypher physical runtime for valid-time point and range queries,
with global `TRAIL`, bounded spill/cancellation, complete projection, and
`EXPLAIN ANALYZE` evidence.

**Architecture:** Generalize the existing ordered `PhysicalExpandSpec` vector
so every syntax segment carries its own `[min_hops,max_hops]` and optional path
slot. Replace the runtime's global fixed-versus-variable branch with a segmented
frontier that completes each segment before feeding terminal states to the next
segment, while carrying one visited-edge set and temporal domain across the
whole pattern.

**Tech Stack:** C++17, Cedar `ColumnBatch`/typed physical plan, pinned temporal
scan, query memory accounting, `QuerySpillFile`, GoogleTest, ASAN/UBSAN/TSAN.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Build every target with `-j1`.
- Keep `ExecuteTcypher()` as the only production graph-query entry point.
- Do not add a scalar, materializing, legacy, or compatibility fallback.
- Keep database format number 1 and do not change any on-disk database format.
- Variable bounds must be finite; global `TRAIL` forbids repeating an edge
  identity anywhere in the complete mixed pattern.
- Support mixed point and valid-time range state queries. Mixed-path `CHANGES`
  remains a specific, tested `NotSupported` contract.
- Direct property dereference on a variable relationship list remains a
  specific bind-time `NotSupported` contract.
- Focused sanitizer evidence does not satisfy the final six-design full-matrix
  release gate.

---

### Task 1: Lock the Support Matrix with RED Tests

**Files:**

- Modify: `tests/test_correctness_kernel.cc`
- Create: `docs/superpowers/plans/2026-07-23-cedar-tcypher-support-matrix.md`

**Interfaces:**

- Consumes: `ExecuteTcypher(std::string, TcypherExecutionContext)` and existing
  `MakeTcypherBinderSchemaSnapshot()` test fixture.
- Produces: exact supported/unsupported query-shape rows and failing point/range
  tests that later tasks must make green.

- [x] **Step 1: Add the support-matrix document**

Create rows for root point/range/change, one fixed Expand, one variable Expand,
all-fixed multi-hop, mixed fixed/variable point, mixed fixed/variable range,
mixed change, fixed relationship property, variable relationship property,
and `EXPLAIN ANALYZE`. Each row must contain parser, binder, physical planner,
runtime, projection, negative contract, and test evidence columns. Mark mixed
point/range `implementation in progress`; mark mixed change and variable-list
property with their exact approved `NotSupported` contract.

- [x] **Step 2: Add variable-then-fixed and fixed-then-variable RED tests**

Use real committed-event fixtures with queries shaped as:

```cpp
"FOR VALID_TIME AS OF 10 "
"MATCH (a)-[p:KNOWS*1..2]->(b)-[r:WORKS_WITH]->(c) "
"RETURN a, p, b, r, c;"
```

and:

```cpp
"FOR VALID_TIME AS OF 10 "
"MATCH (a)-[r:KNOWS]->(b)-[p:WORKS_WITH*1..2]->(c) "
"RETURN a, r, b, p, c;"
```

Assert row count, terminal node IDs, ordered path edge IDs, and the fixed
relationship struct. Do not weaken the assertions to status-only checks.

- [x] **Step 3: Add a mixed-range RED test**

Create different edge/endpoint successor boundaries and assert the exact true
intersection `[valid_from,valid_to)` for both variable and fixed segments.

- [x] **Step 4: Run RED**

Run:

```bash
cmake --build build-current -j1 --target test_correctness_kernel
build-current/tests/test_correctness_kernel \
  --gtest_filter='TcypherExecutorTest.PhysicalMixed*'
```

Expected: the new tests fail with the current deterministic mixed-chain
`NotSupported` result.

---

### Task 2: Generalize Binder and Physical Plan Per Segment

**Files:**

- Modify: `src/tcypher/binder.cc`
- Modify: `include/cedar/tcypher/physical_plan.h`
- Modify: `src/tcypher/physical_plan.cc`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**

- Consumes: `MatchRelationshipPattern::{variable_length,min_hops,max_hops}`.
- Produces: `PhysicalExpandSpec` entries with exact per-segment bounds and
  `path_slot`; mixed statements become `fixed_expand_point_candidate` or
  `fixed_expand_range_candidate` without introducing a new fallback flag.

- [x] **Step 1: Add planner RED tests**

Bind and plan both mixed orders. Assert:

```cpp
ASSERT_EQ(plan->expand_steps().size(), 2U);
EXPECT_EQ(plan->expand_steps()[0].min_hops, expected_first_min);
EXPECT_EQ(plan->expand_steps()[0].max_hops, expected_first_max);
EXPECT_EQ(plan->expand_steps()[1].min_hops, expected_second_min);
EXPECT_EQ(plan->expand_steps()[1].max_hops, expected_second_max);
EXPECT_NE(plan->expand_steps()[variable_index].path_slot.value, 0U);
EXPECT_EQ(plan->expand_steps()[fixed_index].path_slot.value, 0U);
```

Add a fingerprint regression proving that changing one segment bound changes
`plan_id()`.

- [x] **Step 2: Broaden binder candidate legality**

Replace the all-fixed multi-hop predicate with a finite-segment predicate:

```cpp
const auto finite_segment = [](const MatchRelationshipPattern& relationship) {
  return !relationship.variable.empty() && relationship.min_hops > 0 &&
      relationship.max_hops >= relationship.min_hops;
};
```

Apply the existing property/projection restrictions per binding. For variable
relationship bindings, accept only binding projection and valid-time bounds;
reject direct property access with the exact support-matrix message.

- [x] **Step 3: Populate every `PhysicalExpandSpec`**

When constructing `expand_steps`, copy `min_hops/max_hops`; allocate a Binary
path slot only when `variable_length` is true. Keep fixed segment identity and
provenance slots unchanged.

- [x] **Step 4: Extend plan fingerprint and validation**

Mix segment bounds and path slot into `PlanFingerprint`. In
`ValidatePhysicalPlan`, reject zero/reversed bounds, fixed segments with a path
slot, and variable segments without a path slot.

- [x] **Step 5: Run planner GREEN**

Run:

```bash
cmake --build build-current -j1 --target test_correctness_kernel
build-current/tests/test_correctness_kernel \
  --gtest_filter='TcypherBinderTest.*Mixed*:TcypherPhysicalPlanTest.*Mixed*'
```

Expected: all new binder/planner tests pass; runtime tests from Task 1 remain
RED because dispatch has not yet changed.

---

### Task 3: Implement Segmented Point Frontier

**Files:**

- Modify: `src/tcypher/runtime/query_runtime.cc`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**

- Consumes: ordered `PhysicalPlan::expand_steps()` with per-segment bounds.
- Produces: point-query chain rows containing nodes, fixed edges, variable
  segment paths, one global visited-edge set, and a current segment/hop cursor.

- [x] **Step 1: Define the segmented state**

Use a state equivalent to:

```cpp
struct SegmentedPathState {
  std::vector<RawTemporalFact> nodes;
  std::vector<std::vector<RawTemporalFact>> segment_edges;
  std::set<LogicalKey> visited_edges;
  uint32_t segment_index = 0;
  uint32_t segment_hops = 0;
  uint64_t valid_from = 0;
  uint64_t valid_to = kTemporalInfinity;
};
```

Reuse the existing stable edge identity representation; do not derive identity
from physical row locations.

- [x] **Step 2: Replace global variable/fixed dispatch**

For each segment, expand the current terminal node. Fixed `[1,1]` segments emit
only states advanced to the next segment. Variable segments retain states for
another hop until `max_hops` and also emit a copy advanced to the next segment
once `min_hops` is reached.

- [x] **Step 3: Enforce global `TRAIL`**

Before appending an edge, check the state-wide visited set. Add a regression in
which the fixed segment attempts to reuse an edge already consumed by the
variable segment; assert that the row is absent while vertex repetition remains
legal.

- [x] **Step 4: Project mixed bindings**

Build one `ListVector` path value for each variable segment and one
relationship `StructVector` for each fixed segment. Endpoint node bindings stay
Int64 identity slots. Preserve output column order and result-kind metadata.

- [x] **Step 5: Verify point GREEN and morsel determinism**

Run the Task 1 point tests with batch capacities 1 and 64 and compare ordered
results. Expected: both mixed orders pass with identical results across morsel
sizes.

---

### Task 4: Implement Segmented Range Frontier and Property Alignment

**Files:**

- Modify: `src/tcypher/runtime/query_runtime.cc`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**

- Consumes: the segmented state from Task 3 and pinned raw temporal scans.
- Produces: true mixed-path intervals and demanded endpoint/fixed-relationship
  property values.

- [x] **Step 1: Make range chain building segment-aware**

Replace the current global `variable` boolean in `BuildRangeExpandChains` with
the state's `segment_index/segment_hops`. Each expansion intersects:

```text
state interval
INTERSECT edge existence interval
INTERSECT source existence interval
INTERSECT target existence interval
```

Emit a completed segment state only when its hop count reaches `min_hops`.

- [x] **Step 2: Preserve the complete matched result interval**

Carry one result interval intersected across all completed segments and
demanded properties. `valid_from(variable_binding)` and
`valid_to(variable_binding)` project that same demand-aligned complete-match
domain, consistent with existing fixed multi-hop range behavior.

- [x] **Step 3: Generalize property-key lookup**

Map node bindings using the exact node position and fixed relationship bindings
using the exact segment edge. Do not map a variable binding to only its last
edge. Reject variable-list property access before runtime.

- [x] **Step 4: Add range property/predicate tests**

Cover intermediate endpoint property projection, terminal endpoint predicate,
and fixed relationship property projection after a variable segment. Assert
boundary splitting and null property intervals.

- [x] **Step 5: Run range GREEN**

Run:

```bash
build-current/tests/test_correctness_kernel \
  --gtest_filter='TcypherExecutorTest.PhysicalMixed*Range*'
```

Expected: exact interval, projection, and predicate tests pass.

---

### Task 5: Spill, Cancellation, and EXPLAIN ANALYZE

**Files:**

- Modify: `src/tcypher/runtime/query_runtime.cc`
- Modify: `src/tcypher/executor.cc`
- Modify: `src/tcypher/physical_plan.cc`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**

- Consumes: segmented point/range frontier.
- Produces: checksummed spill round-trip, per-segment operator counters, and no
  generic physical range/change user-facing rejection.

- [x] **Step 1: Extend frontier spill framing**

Encode/decode segment index, hop count, all segment edge lists, node list,
global visited identities, and the current result bounds. Reject truncated, oversized,
or inconsistent records as `Corruption`. Charge decoded state before retention.

- [x] **Step 2: Add forced-spill RED/GREEN test**

Use a low query memory limit and bounded descriptor grant. Assert result
equivalence with the no-spill run, positive spill bytes, descriptor release,
and removal of query-private spill files after stream destruction.

- [x] **Step 3: Add cancellation test**

Cancel after the first variable frontier partition and assert
`QueryCancelled`, no later fixed-segment scan, released memory/descriptors, and
a stable terminal status.

- [x] **Step 4: Attribute operators per syntax segment**

Record fixed and variable segment input/output rows under distinct Expand
occurrences. Frontier metrics must identify the variable segment occurrence,
not collapse all segments into occurrence zero.

- [x] **Step 5: Replace the generic EXPLAIN dispatch rejection**

After a physical plan is built, compute physical runtime legality from the plan
shape itself. If no dispatcher exists, return:

```cpp
Status::Corruption(
    "T-Cypher executor",
    "physical plan has no matching runtime dispatcher")
```

Do not return the old generic range/change `NotSupported` message.

- [x] **Step 6: Add EXPLAIN ANALYZE matrix tests**

Cover root range, valid-time change, system-time change, fixed range, fixed
change, all-fixed multi-hop range, one-variable range, and both mixed orders.
Assert plan kind, operator occurrence count, non-zero measured counters, storage
and memory sections, frontier fields for variable segments, and terminal `OK`.

---

### Task 6: Negative Contracts, Oracle, and Focused Gates

**Files:**

- Modify: `src/tcypher/binder.cc`
- Modify: `src/tcypher/executor.cc`
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `docs/superpowers/plans/2026-07-23-cedar-tcypher-support-matrix.md`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**

- Produces: stable unsupported contracts and current evidence without claiming
  the final cross-design release gate.

- [x] **Step 1: Add exact negative tests**

Assert mixed-path `CHANGES` returns:

```text
NotSupported: mixed variable/fixed paths are not supported in CHANGES mode
```

Assert `p.property` where `p` is variable length returns:

```text
NotSupported: property access on a variable relationship path is not supported
```

- [x] **Step 2: Add an independent scalar oracle**

Enumerate finite paths from the committed-event fixture independently of the
physical planner/runtime. Compare ordered point rows and exact range intervals
for both mixed orders, before and after changing batch capacity.

- [x] **Step 3: Run focused normal**

Run:

```bash
cmake --build build-current -j1 --target test_correctness_kernel
build-current/tests/test_correctness_kernel \
  --gtest_filter='TcypherBinderTest.*Mixed*:TcypherPhysicalPlanTest.*Mixed*:TcypherExecutorTest.PhysicalMixed*:TcypherExecutorTest.*ExplainAnalyze*' \
  --gtest_brief=1
```

- [x] **Step 4: Run focused sanitizers**

Rebuild and run the same focused filter under `build-current-asan`,
`build-current-ubsan`, and `build-current-tsan`, always with `-j1`.

- [x] **Step 5: Run complete normal single-process regression**

Run:

```bash
build-current/tests/test_correctness_kernel --gtest_brief=1
```

- [x] **Step 6: Static checks and evidence update**

Run `git diff --check`, touched-file trailing-whitespace scans, and searches for
the removed generic range/change message and any new legacy/V2 fallback. Update
the support/completion matrices with exact test counts and commands. State
explicitly that full ASAN/UBSAN/TSAN, HTAP scheduler stress, benchmark
reproducibility, and paper artifacts remain open.

No Git integration action is performed.
