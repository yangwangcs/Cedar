# Task 12 Report

Status: NEEDS_CONTEXT

Implementation summary:

- Added `TemporalTraversal`, temporal edge visibility, direction/type filtering,
  half-open interval intersection, endpoint state checks, QueryDelta overlay,
  point/interval K-hop labels, minimum-depth deduplication, and reservation
  accounting in `graph_frontier`.
- Wired `Query::Expand` and bounded `Query::KHopExpand` through AnalyzeQuery and
  QueryRuntime, retaining source/edge/destination slots and edge-property
  clipping.
- Added `Snapshot::ScanFamily`/`FactStore::ScanFamily` so canonical identity
  fallback reads all home partitions. Added a projection callback seam and
  candidate counter for a bounded adjacency seek.
- Added cross-partition, public expansion, and K-hop fixture coverage.

Commit: `d73c6150cbb61340f4fa4fcda3bd6d34112d6de3`

Verification:

```text
cmake --build build/query-debug -j2 --target \
  test_temporal_expand test_query_planner test_query_canonical test_kernel_snapshot
  all targets built successfully

ctest --test-dir build/query-debug --output-on-failure -R \
  'TemporalExpand|QueryPlanner|QueryCanonical'
  32 passed, 0 failed

build/query-debug/tests/test_temporal_expand --gtest_color=no
  4 tests passed
```

Remaining concerns:

- The current projection format exposes adjacency entity/page metadata but no
  source/target endpoint payload or seek API. Runtime therefore uses the
  correctness-first canonical family scan when no QueryDelta identities are
  available. The callback seam is bounded and tested, but cannot be wired to a
  real EdgeOut/EdgeIn posting without that upstream interface. Consequently
  the required selected-degree physical-work bound is not yet met.
- `KernelSnapshotTest.MultiExistsPreservesRequestOrder` was observed once in
  the broader focused regex with `multi_get_operations=5` vs `3`; it is outside
  the Task 12 changes and passed/was not reproduced in the final Task 12 regex.

## Task 12 Review Fixes

- Fixed `KHopExpand` endpoint selection for `kBoth`: incoming traversal now
  reaches the stored source, outgoing reaches the stored target, and self-loops
  are emitted once. Minimum-depth labels suppress repeated paths and subtract
  overlapping equal-depth half-open intervals before emitting witnesses;
  incoming predecessors point back to the frontier vertex.
- Added public `ExpandSpec.edge_type` propagation and graph edge-property
  binding after traversal materialization, preserving property type/missing
  semantics and intersecting property valid-time with traversal intervals.
- Added graph abort/deadline checks at posting, fallback identity, delta, state
  interval, and frontier label boundaries. Pinned projection generation is
  passed to `AdjacencyIndex::Seek`; an adjacency `NotFound` performs one
  authoritative canonical fallback under the interactive candidate bound.
- Added `KHopBothUsesFrontierEndpointAndDeduplicatesDiamond` regression coverage.

Verification after review fixes:

```text
cmake --build build/query-debug --clean-first -j1 --target \
  test_temporal_expand test_query_canonical test_query_planner test_kernel_snapshot
  all targets built successfully

ctest --test-dir build/query-debug --output-on-failure -R \
  'TemporalExpand|QueryCanonical|QueryPlanner|KernelSnapshot'
  38 passed, 1 failed: KernelSnapshotTest.MultiExistsPreservesRequestOrder
  (multi_get_operations=5, expected 3; pre-existing/outside Task 12)
```
