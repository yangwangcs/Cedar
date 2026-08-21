# Task 13 Public Edges Fix Report

## Changes

- Coexisting public target discovery now uses non-charging `KHopExpand` for
  `max_hops > 1`, collecting every reachable non-seed label; one-hop discovery
  remains `ExpandTemporal`.
- One-hop endpoint selection now derives the opposite endpoint from the seed,
  including incoming-only traversals for `ExpandDirection::kBoth`.
- Coexisting winner selection compares interval objective, hop depth, and edge
  lexicographic order against all overlapping winners, replacing every winner
  beaten by a candidate while preserving disjoint intervals.
- Added public both-direction, multi-hop, and overlapping-winner regressions.

## Verification

```text
cmake --build build/query-debug -j2 --target test_coexisting_path test_temporal_expand
ctest --test-dir build/query-debug --output-on-failure -R 'CoexistingPath|TemporalExpand'
100% tests passed, 0 tests failed out of 21
```
