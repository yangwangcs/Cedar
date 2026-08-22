# Task 14 Forward Label Fix Report

## Scope

The Task 14 quality review found unsound forward label pruning for
`EarliestArrival` and `FastestDuration`: an earlier arrival could be retained
over a later arrival even when the intermediate vertex was invisible during
the required waiting interval.

## Changes

- `EarliestArrival` now keys labels by exact `(vertex, normalized depth,
  arrival time)`. It only collapses duplicate timed states, so a later label
  survives a visibility gap and remains eligible for expansion.
- `FastestDuration` no longer applies departure/arrival Pareto dominance
  across labels without proving visibility continuity. It retains exact
  `(vertex, normalized depth, departure, arrival)` states and still enforces
  `max_labels`, graph-label reservations, and interval-fragment accounting.
- Added regression coverage for both objectives with an intermediate vertex
  visible on `[0,5) U [10,100)`, where the early label cannot wait but the late
  label reaches the target.

## Verification

```text
cmake --build build/query-debug -j2 --target \
  test_temporal_journey test_coexisting_path test_query_types
ctest --test-dir build/query-debug --output-on-failure \
  -R 'TemporalJourney|CoexistingPath|QueryTypes'
100% tests passed, 37/37
```

The 200-seed exhaustive temporal journey oracle is included in this gate and
passed. `git diff --check` also passed for the implementation and test files.

