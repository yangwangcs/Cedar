# Task 14 Report: Temporal Journeys

## Result

`DONE_WITH_CONCERNS`

Implemented the temporal journey result model, flat `JourneyColumn`, duration
overflow and half-open traversal checks, bounded-hop earliest-arrival search,
latest-departure and fastest-duration execution, public journey builders, and
runtime materialization through the existing graph execution path. Duration
values are read from authoritative Snapshot edge properties or an internal
duration callback. Journey labels retain predecessor IDs and materialize only
the selected result.

## Verification

```text
cmake --build build/query-debug -j2 --target test_temporal_journey test_coexisting_path test_query_types
ctest --test-dir build/query-debug --output-on-failure -R 'TemporalJourney|CoexistingPath|QueryTypes'
100% tests passed, 0 tests failed out of 21
```

## Concerns

LatestDeparture and FastestDuration currently enumerate bounded departure
times rather than using the planned reverse adjacency and Pareto frontier
algorithms. Prepare-time FIFO proof and a 200-seed exhaustive oracle are also
not yet present. These are correctness-preserving for the bounded deterministic
fixtures covered here, but remain follow-up work for full Task 14 conformance.
