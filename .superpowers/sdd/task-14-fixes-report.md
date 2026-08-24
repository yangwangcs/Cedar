# Task 14 Fixes Report

## Result

`DONE_WITH_CONCERNS`

The three final-gate correctness findings are fixed and covered by focused
regressions. The Task 14 brief's 200-seed exhaustive oracle is still not
implemented in this change and remains a follow-up acceptance item.

## Changes

- Earliest Arrival, Latest Departure, and Fastest Duration now key labels by
  `(vertex, depth)` whenever `max_hops` is bounded. With an unbounded hop
  limit, depth is folded to zero to retain the prior finite-state protection
  against zero-duration cycles.
- Latest Departure no longer assumes that the lower endpoint is feasible or
  that callback feasibility is monotone. It scans candidate departure times
  backward from the latest bound, so a duration callback that is missing at
  the lower endpoint but becomes valid later is handled correctly.
- Journey interval-fragment accounting is now cumulative for the complete
  query execution. A shared per-query counter returns `ResourceExhausted`
  when the aggregate exceeds `max_interval_fragments`.
- Added regressions for depth-sensitive Earliest/Fastest paths, missing-then-
  valid Latest Departure callbacks, and multi-expansion fragment exhaustion.

## Verification

Commands:

```text
cmake --build build/query-debug -j2 --target test_temporal_journey test_coexisting_path test_query_types
ctest --test-dir build/query-debug --output-on-failure -R 'TemporalJourney|CoexistingPath|QueryTypes'
```

Output: build succeeded; `100% tests passed, 0 tests failed out of 31`.

## Remaining risks

- Callback reverse search is deliberately bounded by the existing FIFO proof
  limit (one million integer time points); very wide callback intervals are
  rejected during validation.
- Registered property durations are still conservatively rejected by public
  query preparation because their FIFO proof is unavailable.
- The required deterministic 200-seed exhaustive oracle has not yet been
  added.
