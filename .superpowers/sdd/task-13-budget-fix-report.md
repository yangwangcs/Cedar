# Task 13 Budget Fix Report

## Root Cause

`MaterializeGraphRows` used the live `QueryReservation` while calling
`ExpandTemporal` to discover candidate targets for coexisting paths. That
charged raw graph traversals before `CoexistingShortestPath` charged its
surviving labels and interval fragments, so each accepted edge could consume
both budgets twice.

## Changes

- Candidate discovery now clones `GraphFrontierOptions` with
  `reservation = nullptr`, preserving delta, cancellation, generation, and
  fallback settings.
- `CoexistingShortestPath` remains the sole charging path for initial and
  surviving labels/fragments.
- Added public Database -> Vertex query -> `Query::CoexistingShortestPath` ->
  `PrepareQuery`/`Execute` -> `QueryBatch::Get<PathValue>` coverage, including
  a tight two-label/two-fragment budget, empty target behavior, and public
  type-mismatch behavior.

## Verification

```text
cmake --build build/query-debug -j2 --target test_coexisting_path test_temporal_expand
ctest --test-dir build/query-debug --output-on-failure -R 'CoexistingPath|TemporalExpand'
```

The focused build and test suite passed. The requested exhaustive oracle over
at least 200 deterministic seeds was not added in this fix; no result is being
claimed for that remaining spec gap.

## Commit

See the commit containing this report and the implementation changes.
