# Task 14 Final Fixes Report

## Scope

This pass addressed the remaining Task 14 review findings:

- LatestDeparture now checks the requested final target only at the arrival
  instant. Intermediate reverse-search vertices still require continuous
  visibility while waiting for the next edge.
- LatestDeparture now retains all feasible incoming candidates, ordered by
  latest departure, instead of greedily retaining one candidate per reverse
  expansion. This preserves paths that satisfy `max_hops` and can return to
  the requested source.
- Added a deterministic 200-seed exhaustive oracle. It independently
  enumerates bounded walks, half-open intervals, edge gaps, ties, hop limits,
  Pareto candidates, and a callback addition-overflow case, then compares all
  three journey objectives.
- Added public query planning regressions for LatestDeparture `kIn` and
  `kBoth` directions.

## Verification

```text
cmake --build build/query-debug -j2 --target \
  test_temporal_journey test_coexisting_path test_query_types

ctest --test-dir build/query-debug --output-on-failure \
  -R 'TemporalJourney|CoexistingPath|QueryTypes'

100% tests passed, 0 tests failed out of 34
```

The exhaustive 200-seed oracle passed as
`TemporalJourneyTest.ExhaustiveOracleMatchesThreeObjectivesFor200Seeds`.
