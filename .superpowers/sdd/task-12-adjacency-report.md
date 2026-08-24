# Task 12 Adjacency Physical-Work Report

Status: COMPLETE for the Cedar-owned adjacency runtime bound.

## Implementation

- `AdjacencyIndex` is a Cedar-owned in-memory posting index keyed by
  `VertexRef`, direction, and optional edge type. Each posting entry retains
  the authoritative commit sequence and generation.
- Database open builds the index from `FactFamily::kEdgeIdentity` through the
  existing authoritative `FactStore::ScanFamily` API. No RocksDB handle or
  ownership is exposed to query runtime code.
- `QueryDeltaView` identities are merged at seek time and filtered by the
  pinned snapshot commit sequence. A pinned generation can also be selected.
- `GraphFrontier` seeks only selected postings. If coverage is missing, it
  returns to an explicit canonical fallback; runtime interactive queries cap
  fallback candidates at 4096 and analytical callers may leave the bound
  unlimited. The canonical scan itself stops once a finite bound is exceeded.
- Snapshot exposes the immutable cache pointer, and `QueryRuntime` passes it
  into every graph expansion and k-hop frontier.

## Verification

RED was observed before implementation: the new regression linked with
undefined `AdjacencyIndex::ApplyDelta`/`Seek` symbols.

The focused serial build and test command was:

```text
cmake --build build/query-debug --target test_temporal_expand test_query_planner test_kernel_snapshot -j1
ctest --test-dir build/query-debug --output-on-failure -R 'TemporalExpand|QueryPlanner|KernelSnapshot'
```

Task12, planner, and snapshot tests passed except the pre-existing flaky
`KernelSnapshotTest.MultiExistsPreservesRequestOrder` (`multi_get_operations`
5 vs expected 3). The new degree-10/100000-edge regression passes and verifies
selected posting candidates, direction/type filtering, self-loop deduplication,
and multi-edge preservation.
