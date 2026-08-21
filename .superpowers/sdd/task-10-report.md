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
- Coverage slices now carry region kind/part/property/schema/entity bounds and
  database identity. ReadChains verifies the selected generation/base and
  returns Conflict if a generation was retired or replaced; this prevents a
  reader from silently switching to `current_`.
- Runtime checks canonical source status before mixing and propagates derived
  reader errors other than an explicit unavailable/fallback status.
- `PreparedQueryPlan` now carries a bound `PhysicalPlan` and a Cedar-owned
  projection reader. Projection slices are decoded into `RuntimeRow` intervals
  by `QueryRuntime`; canonical and delta-fallback slices are read from the
  canonical source, clipped to their own half-open ranges, and concatenated
  without overlap before the existing predicate/property/output pipeline.

Verification after fixes:

```text
cmake --build build/query-debug -j2 --target test_query_planner test_projection_store: PASS
ctest --test-dir build/query-debug --output-on-failure -R 'QueryPlanner|QueryCanonical|QueryDelta|ProjectionStore': 43/43 PASS
git diff --check: PASS
```

Delta merge is enabled only when the database owns a live QueryDelta view and
the planner proves `(base,S]` continuity; otherwise it marks
`delta-fallback` and the physical executor uses canonical slices.

The runtime now has the per-entity synthetic-boundary adapter: each delta slice
builds Put/Delete boundaries from projection intervals, acquires the exact
Delta view through the snapshot, calls `MergeBoundaries`, and materializes the
corrected state intervals before clipping and concatenation. Partial entity
coverage remains canonical by construction.

The acquired contiguous Delta view is now stored as an immutable shared view in
the prepared execution plan; runtime does not reacquire a moving maintenance
view during materialization.

Runtime follow-up after review:

- `QueryRuntime` now collects every projection interval from all returned
  `ProjectionChain` pages by `(part, entity, property)` before one
  `MergeBoundaries` and `MaterializePresentState` pass. This prevents duplicate
  or truncated state when a logical fact spans chains. Delta-merge slices are
  selected from derived rows alongside projection slices; canonical slices
  continue to use the canonical source.
- Added a real `PreparedQueryPlan`/`QueryRuntime::Execute` fixture with two
  cross-chain projection segments and an immutable bound delta view. It compares
  mixed projection+delta output with canonical-only output at valid times 4, 5,
  10, and 15 and asserts values `1, 3, 2, 4` with no duplicate rows.
- Plan copy-assignment now preserves both the bound delta view and delta reader,
  so execution cannot silently reacquire a moving maintenance view.

Final review follow-up:

- Planner detects any overlapping valid-time regions with different
  `(kind, part, property, schema)` keys and emits one canonical fallback; the
  one-dimensional `CoverageSlice` cannot represent those parallel key domains.
  Empty manifest regions likewise fall back to canonical access.
- Physical execution clips every canonical and derived row to each half-open
  coverage slice before concatenation. This prevents a full canonical interval
  from being duplicated across projection gaps, including when the projection
  reader is unavailable and canonical fallback is selected.
- Physical explain now reports pushdowns, per-slice generation/base metadata,
  fallback sources, and confidence (`known` versus `conservative`), with root
  projection metadata and logical child descriptions populated.
- Projection readers retain generation/base checks and return `Conflict` if a
  rollover changes the requested generation; no silent generation switch is
  permitted. This is conflict-safe validation, not a claimed pinned handle.

Final Important follow-up: projection execution now pins the planned
`ProjectionGeneration` in `PreparedQueryPlan`. After physical binding,
`PreparedQuery::Execute` acquires the exact generation/base/identity/key
coverage described by each derived slice and returns `Conflict` if rollover
occurs during planning. The projection reader carries that immutable handle
and reads the pinned manifest/segment directory through the store overload;
it never re-resolves `PROJECTION-CURRENT`. Retired generations remain readable
while the plan/reader holds a pin, while store close or an invalidated handle
returns an explicit unavailable status. Added rollover and close invalidation
coverage tests in `test_projection_store.cc`.

Verification from the task worktree:

```text
cmake --build build -j2 --target cedar_core test_query_planner test_query_canonical test_query_delta test_projection_store test_query_relational: PASS
./build/tests/test_query_planner: 10/10 PASS
./build/tests/test_query_canonical: 18/18 PASS
./build/tests/test_query_delta: 10/10 PASS
./build/tests/test_projection_store: 11/11 PASS
./build/tests/test_query_relational: 41/41 PASS
git diff --check: PASS
```
