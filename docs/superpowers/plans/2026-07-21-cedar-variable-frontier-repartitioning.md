# Cedar Variable Path Frontier Repartitioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute root point variable-length paths correctly and repartition point/range path frontiers deterministically as observed frontier size grows.

**Architecture:** Point variable Expand uses breadth-first path states containing the root, current endpoint, last edge provenance, and the exact visited-edge trail set. Before each hop, point and range frontiers choose a power-of-two partition count from `frontier_size / batch_capacity`, bounded to eight partitions, group work by current endpoint for adjacency locality, and merge child states back in parent order so repartitioning does not change deterministic output order. Completed point paths are published through the existing fixed-Expand gather/filter/project pipeline using their terminal endpoint and last edge.

**Tech Stack:** C++17, `QueryRuntimeState`, pinned temporal scans, `PhysicalExpandSpec`, `ColumnBatch`, GoogleTest, EXPLAIN ANALYZE.

## Global Constraints

- Variable point paths honor `min_hops` and `max_hops`.
- Trail semantics prohibit reusing the same complete logical edge identity in one path.
- Vertex repetition remains legal when no edge repeats.
- One output row is retained per distinct path; equal endpoints are not deduplicated.
- Repartitioning is deterministic, bounded to eight partitions, and does not change parent/edge output order.
- Target existence is validated at the query's pinned point snapshot before extending a path.
- Terminal endpoint predicates still pass through normal PropertyGather and final Filter.
- Range paths preserve their existing temporal interval intersections and property-boundary alignment.
- Frontier spill and exact resource charging remain deferred to the final constraint phase.
- Preserve the dirty worktree; do not reset, clean, commit, or push.

---

### Task 1: Reproduce missing point variable execution

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

- [ ] Add `PhysicalVariablePointExpandHonorsTwoHopMinimum` with vertices `1,2,3`, edges `1->2` and `2->3`, and query:

```cypher
FOR VALID_TIME AS OF 10
MATCH (a)-[r:KNOWS*2..2]->(b)
RETURN a, b;
```

- [ ] Assert the only result is `(1,3)`. Run it and observe RED because the current point runtime emits the one-hop endpoint `2`.

### Task 2: Add the point path frontier

**Files:**
- Modify: `src/tcypher/runtime/query_runtime.cc`

- [ ] Add `PointVariablePath` containing `root_id`, `current_id`, terminal edge fields, and `std::set<LogicalKey> visited_edges`.
- [ ] Add a pinned point adjacency loader that resolves visible edges for one current endpoint and validates visible target existence.
- [ ] Build breadth-first frontiers from each root row through `max_hops`, reject repeated logical edges, add paths to the completed set at every hop in `[min_hops,max_hops]`, and preserve one state per distinct path.
- [ ] Publish completed paths in the same system-column layout used by fixed point Expand, then call `ContinuePointExpandPredicates` so existing relationship/endpoint gather, filters, provenance, projections, and sinks remain authoritative.
- [ ] Route both the initial root batch and the source-predicate continuation to variable frontier execution whenever `expand.max_hops > 1`.
- [ ] Run the two-hop RED test GREEN.

### Task 3: Deterministic frontier repartitioning

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/tcypher/executor.cc`
- Modify: `src/observability/explain_analyze_profile.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`

- [ ] Add execution/profile counters for hops, input states, next-frontier states, completed paths, repartitions, processed partitions, and maximum partition size.
- [ ] Add a shared helper that selects `1,2,4,8` partitions until `frontier_size <= partitions * batch_capacity` or eight is reached.
- [ ] Partition by a stable mix of the current endpoint ID, process endpoints grouped by partition, retain children per original parent index, and flatten in original parent order.
- [ ] Apply the helper to point variable frontiers and existing variable range `ChainRow` frontiers without changing range interval logic.
- [ ] Emit the counters in a new `frontier` EXPLAIN ANALYZE object.

### Task 4: Trail, multiplicity, point/range repartition regression

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `.superpowers/sdd/progress.md`

- [ ] Add a point cycle test proving `1->2->1->3` is accepted at three hops but the first edge cannot be reused.
- [ ] Add a branching point test with `batch_capacity=1` whose second-hop frontier grows to four states; assert four terminal paths, at least one repartition, and at least four processed partitions.
- [ ] Add the equivalent branching valid-time range test and assert unchanged intervals plus nonzero frontier repartition counters.
- [ ] Extend EXPLAIN ANALYZE coverage for the `frontier` object.
- [ ] Run variable/fixed point and range Expand focused tests, broad CBO/Explain tests, normal CTest, `git diff --check`, and a trailing-whitespace scan.
- [ ] Record exact counts and deferred spill/resource/sanitizer work without committing.
