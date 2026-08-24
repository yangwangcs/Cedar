# Task 14 Latest-Departure Label Fix

## Result

Fixed the reverse LatestDeparture label pruning that discarded an earlier
departure whenever a later label reached the same `(vertex, depth)`. Reverse
departure times are not dominance-equivalent because an intermediate vertex
can contain a visibility gap during the waiting interval.

The search now retains one label per exact `(vertex, normalized depth,
departure time)` state. Exact duplicates remain collapsed, including repeated
zero-duration states when `max_hops` is unbounded, while labels at different
times remain available for later continuous-visibility validation.

Added `LatestDepartureRetainsLabelsAcrossVisibilityGap`, covering:

- `b` visibility `[0,18) U [20,100)`;
- a late `b -> d` candidate that requires waiting across the gap;
- an earlier `b -> d` candidate that yields the valid `a@14 -> b@15 -> d@16`
  journey.

The regression uses `[15,17)` for the short edge because Cedar traversal
intervals are half-open and a positive duration of `1` must finish strictly
before the exclusive upper bound. `[15,16)` therefore cannot admit arrival at
`16` under the existing interval contract.

## Verification

```text
cmake --build build/query-debug -j2 --target \
  test_temporal_journey test_coexisting_path test_query_types
ctest --test-dir build/query-debug --output-on-failure \
  -R 'TemporalJourney|CoexistingPath|QueryTypes'
100% tests passed, 35/35
```

`git diff --check` passes for the task changes. The pre-existing user change in
`.superpowers/sdd/task-12-report.md` was left untouched.
