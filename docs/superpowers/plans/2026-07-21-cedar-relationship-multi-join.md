# Cedar Relationship-Capable Multi-MATCH Join Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route connected multi-`MATCH` point queries whose children contain relationship expansion through the typed, spill-capable `PhysicalMultiHashJoinPlan`, with an independently resolved `SYSTEM_TIME AS OF` snapshot for every child.

**Architecture:** Treat each syntactic `MATCH` clause as one physical input subgraph instead of treating each node binding as an input. Build every input with the existing `PlanPhysicalRootPoint` fixed/variable-expand implementation, preserve a typed layout for all node, relationship, property, and temporal-provenance outputs, and join subgraphs through the existing left-deep hash-join steps. Store the resolved snapshot sequence beside each input and derive child `QuerySnapshot` values from the same pinned metadata envelope at runtime.

**Tech Stack:** C++17, Cedar typed T-Cypher IR, `PhysicalPlan`, `PhysicalMultiHashJoinPlan`, `QuerySnapshot`, vectorized `QueryResultStream`, GoogleTest.

## Global Constraints

- No second relationship join executor is introduced; all accepted shapes reuse `OpenPhysicalRootPointRuntime` and `PhysicalHashJoinResultStream`.
- Each binding belongs to exactly one syntactic `MATCH` input; a join equality connects two different inputs.
- Child plans retain node identity, relationship structs, endpoint identity, demanded properties, and temporal provenance required by final output or later joins.
- Every child uses the same pinned VersionSet, schema snapshot, Blob epoch, and statement HLC, but may use a different resolved `snapshot_seq`.
- Only point-state scopes are accepted. Per-child `SYSTEM_TIME AS OF` is supported; system-time ranges/changes and valid-time ranges remain outside this slice.
- Existing relationship-free plans keep identical ordering, fingerprints, EXPLAIN shape, and runtime behavior.
- The reconstructed dirty worktree is preserved; this plan intentionally omits staging and commit steps.

---

### Task 1: Relationship Subgraphs Become Multi-Join Inputs

**Files:**
- Modify: `include/cedar/tcypher/physical_plan.h`
- Modify: `src/tcypher/physical_plan.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: primary `MATCH`, `additional_matches`, `BoundVariable`, `BoundPropertyReference`, `LogicalJoinEdge`.
- Produces: one `PhysicalPlan` and one typed `input_layouts` entry per syntactic `MATCH` clause.

- [ ] **Step 1: Write failing planner tests**

Add tests for a connected query such as:

```text
FOR VALID_TIME AS OF 10
MATCH (a)-[ab:KNOWS]->(b)
MATCH (c)-[cd:KNOWS]->(d)
WHERE b.name = c.name
RETURN a, ab, b, c, cd, d;
```

Assert `CanPlanPhysicalMultiHashJoin`, two child expand plans, one join step, typed layouts for all six bindings, relationship-binding physical type, and no legacy fallback eligibility.

- [ ] **Step 2: Run the focused test and observe RED**

Run:

```bash
cmake --build build-v2 --target test_correctness_kernel -j4
build-v2/tests/test_correctness_kernel --gtest_filter='TcypherPhysicalPlanTest.RelationshipMultiMatch*'
```

Expected: the candidate check rejects relationships.

- [ ] **Step 3: Build explicit input ownership**

In `PlanPhysicalMultiHashJoin`, construct an input descriptor per syntactic clause:

```cpp
struct MatchInput {
  const MatchClause* clause = nullptr;
  std::vector<BindingId> bindings;
  std::vector<BoundTemporalScope> scopes;
  std::vector<InputColumn> columns;
  std::shared_ptr<const PhysicalPlan> plan;
  PhysicalCardinalityEstimate estimate;
  uint64_t snapshot_seq = 0;
};
```

Represent the primary clause through an owned/local `MatchClause`. Build `input_by_binding` for every root, expanded node, and relationship binding and reject duplicates. Logical join edges are converted to input adjacency; same-input equalities are not HashJoin edges.

- [ ] **Step 4: Slice a complete bound child statement**

For every input, copy only its syntax, variables, demanded properties, projections, scopes, exact root id, and expand candidate flags. Reuse the parent `LogicalPlan::demand.variables` entries owned by the input. Call `PlanPhysicalRootPoint`; do not reconstruct expand slot semantics in the multi-join planner.

- [ ] **Step 5: Generalize layout validation and fingerprinting**

Allow an input layout to contain any binding in that input's declared ownership set. Validate every physical projection against the corresponding layout entry, validate unique binding ownership across inputs, include ownership and child plan identities in the parent fingerprint, and preserve the existing relationship-free fingerprint behavior where possible.

- [ ] **Step 6: Verify planner tests**

Run the new tests and all `TcypherPhysicalPlanTest.*` tests with zero failures.

---

### Task 2: Relationship Multi-MATCH Runtime and Routing

**Files:**
- Modify: `src/tcypher/executor.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: relationship-capable `PhysicalMultiHashJoinPlan`.
- Produces: vectorized relationship child streams folded through existing spill-capable joins.

- [ ] **Step 1: Write failing runtime/routing tests**

Cover relationship struct values, incoming/outgoing endpoints, relationship properties used as join keys, final relationship temporal metadata, EXPLAIN, and `legacy_multi_root_materialized_rows == 0`.

- [ ] **Step 2: Observe RED**

Run only the new executor tests. Expected: the current route calls `ExecuteMultiFixedMatchAsOf` and increments the legacy counter.

- [ ] **Step 3: Route accepted relationship shapes**

Remove only the relationship/expanded-node rejection in `CanPlanPhysicalMultiHashJoin`; retain point-scope, projection-kind, connected-input, and typed-key checks. Planner/executor selection continues to call `PlanPhysicalMultiHashJoin` and `OpenPhysicalMultiHashJoinRuntime`.

- [ ] **Step 4: Reuse existing runtime fold unchanged where possible**

Open each complete child with `OpenPhysicalRootPointRuntime`; use the child projection positions recorded in `input_layouts` as hash keys and outputs. The parent applies aggregate/distinct/sort once after final projection.

- [ ] **Step 5: Verify runtime and fallback behavior**

Run relationship multi-join tests, all physical expand tests, all physical hash/multi-hash tests, and legacy fallback counter tests.

---

### Task 3: Independent Per-Child SYSTEM_TIME AS OF

**Files:**
- Modify: `include/cedar/tcypher/physical_plan.h`
- Modify: `src/tcypher/physical_plan.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Adds to `PhysicalMultiHashJoinPlan`:

```cpp
std::vector<uint64_t> input_snapshot_seqs;
```

- [ ] **Step 1: Replace the rejection test with RED support tests**

Create data whose values differ at commit sequences 1, 2, and 3. Query three connected inputs with distinct `FOR SYSTEM_TIME AS OF` cutoffs and assert that each child observes its own historical state.

- [ ] **Step 2: Observe RED**

Run the new planner/runtime tests. Expected: `CanPlanPhysicalMultiHashJoin` returns false and the legacy path is selected.

- [ ] **Step 3: Resolve and store every input cutoff**

For each input, start from the visible prefix or inherited primary system cutoff, apply that input's state scopes, reject cutoffs above the visible prefix, and append the result to `input_snapshot_seqs`. Include the vector in validation, EXPLAIN, and the parent fingerprint.

- [ ] **Step 4: Derive child snapshots**

Before opening an input, copy the parent `QuerySnapshot` and replace the `snapshot_seq` in every resolved temporal context with `input_snapshot_seqs[input_index]`. Preserve `visible_seq_ceiling` and all pinned metadata pointers.

- [ ] **Step 5: Reject unsupported system-time modes explicitly**

Candidate checks continue to reject `CHANGES FOR SYSTEM_TIME` and any range mode. Do not silently reinterpret them as `AS OF`.

- [ ] **Step 6: Verify temporal tests**

Run new per-child tests plus all `*SystemTime*`, `*Temporal*`, and physical multi-join tests.

---

### Task 4: Regression and Progress Ledger

**Files:**
- Modify: `.superpowers/sdd/progress.md`

- [ ] **Step 1: Focused verification**

```bash
cmake --build build-v2 --target test_correctness_kernel -j4
build-v2/tests/test_correctness_kernel --gtest_filter='TcypherPhysicalPlanTest.*MultiJoin*:TcypherExecutorTest.*Multi*Join*:TcypherExecutorTest.*Expand*'
```

- [ ] **Step 2: Expanded and full verification**

```bash
build-v2/tests/test_correctness_kernel
ctest --test-dir build-v2 --output-on-failure
git diff --check
```

- [ ] **Step 3: Record exact functional closure**

Update the ledger with supported relationship shapes, per-child snapshot behavior, test counts, and any intentionally unsupported temporal range/change shapes. Do not mix deferred Sort/HashJoin boundary debt into this functional slice.
