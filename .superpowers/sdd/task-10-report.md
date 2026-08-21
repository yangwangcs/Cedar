# Task 10 Report

Status: PARTIAL: planner correctness and real binding complete; physical
projection execution remains explicitly blocked by the existing runtime row
boundary.

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

Verification after fixes:

```text
cmake --build build/query-debug -j2 --target test_query_planner test_projection_store: PASS
ctest --test-dir build/query-debug --output-on-failure -R 'QueryPlanner|QueryCanonical|QueryDelta|ProjectionStore': 40/40 PASS
git diff --check: PASS
```

Remaining blocker: `QueryRuntime::Execute` still consumes only
`PreparedQueryPlan` and constructs `RuntimeRow` from `TemporalSource`; there is
no existing adapter from decoded `ProjectionChain` to `RuntimeRow`/`BatchStream`,
nor a database-owned reader handle in its signature. `ReadChains` is therefore
not silently wired into output execution. Completing Step 8 requires extending
that runtime boundary and a derived-vs-canonical fixture; until then execution
continues canonically for correctness.
