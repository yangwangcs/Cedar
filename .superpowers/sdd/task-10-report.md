# Task 10 Report

Status: DONE for the supported physical boundary; delta merge remains an
explicit canonical fallback until its merge adapter is enabled.

Implemented the Cedar-owned late-binding planner in
`src/query/planner/query_planner.{h,cc}`. The planner now provides:

- immutable projection catalog, delta, and statistics planning views;
- snapshot-checked, half-open coverage partitioning with explicit canonical
  gaps and projection/delta slices;
- rejection of overlapping coverage and projections newer than the snapshot;
- conservative cost estimates when statistics are absent;
- the approved 4,096-row interactive heuristic and analytical selection for
  broad scans, sort, and aggregate nodes;
- physical operation selection, adjacency pushdown markers, lane exchange
  reporting, spill permission, and structured physical descriptions;
- logical and physical explain formatting.

`PreparedQuery::ExplainLogical` and `ExplainPhysical` were added. Physical
explain intentionally uses an empty derived catalog at the public API boundary,
so it reports canonical fallback truthfully until Database exposes immutable
projection/delta catalog adapters to the query runtime.

Verification:

```text
cmake --build build/query-debug -j2 --target test_query_planner: PASS
ctest -R 'QueryPlanner|QueryCanonical|QueryDelta|ProjectionStore': 38/38 PASS
git diff --check: PASS
```

Follow-up fixes after review:

- `PreparedQuery::Execute` and `ExplainPhysical` now bind against the actual
  immutable `ProjectionStore::current_manifest()` and
  `QueryDelta::AcquireThrough()` views. Incomplete `(base,S]` continuity or
  `first_missing` forces canonical fallback rather than an unconditional
  DeltaMerge claim.
- Planner validates database identity and refuses to collapse disjoint entity
  partitions into a one-dimensional time slice; those regions fall back to a
  canonical slice. `QueryProjectionStore::ReadChains` is a Cedar-owned,
  snapshot/base-checked segment decoder.
- Added incomplete-delta and identity mismatch safety tests.
- `PreparedQueryPlan` now carries a bound `PhysicalPlan` and a Cedar-owned
  projection reader. Projection slices are decoded into `RuntimeRow` intervals
  by `QueryRuntime`; canonical and delta-fallback slices are read from the
  canonical source, clipped to their own half-open ranges, and concatenated
  without overlap before the existing predicate/property/output pipeline.

Verification after fixes:

```text
cmake --build build/query-debug -j2 --target test_query_planner test_projection_store: PASS
ctest --test-dir build/query-debug --output-on-failure -R 'QueryPlanner|QueryCanonical|QueryDelta|ProjectionStore': 41/41 PASS
git diff --check: PASS
```

Delta merge is deliberately disabled in the production planning context until
the runtime has a boundary-event adapter. The planner marks this as
`delta-fallback` and never claims an unmerged `(base,S]` tail is executable;
the physical executor still handles mixed projection/canonical slices.
