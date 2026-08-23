# Public Registered-Duration Journey Fix

## Scope

The public query planner previously rejected every journey using a registered
edge duration property during `PrepareQuery`, because FIFO could not be proved
without a snapshot. That made public `EarliestArrival`, `LatestDeparture`, and
`FastestDuration` plans unreachable even when the snapshot contained a valid
FIFO duration stream.

## Changes

- Removed only the prepare-time FIFO refusal. Existing edge-entity and
  non-negative integer schema checks remain enforced.
- Added snapshot-time loading of corrected duration-property intervals,
  including the uncommitted query delta.
- Added complete FIFO validation for piecewise-constant duration segments:
  segment boundary arrivals are checked, and duration conversion, negative
  values, and arrival-time overflow fail before a traversal is admitted.
- Kept callback-duration FIFO validation unchanged.
- Replaced the old prepare-rejection test with a public execution test that
  proves a registered duration reaches runtime and materializes `JourneyValue`.
- Added a public non-FIFO registered-duration test that prepares successfully
  but fails at execution before a batch is returned.

## Verification

- `cmake --build build/query-debug -j2 --target test_temporal_journey`: passed.
- `test_temporal_journey`: 22/22 passed, including the 200-seed exhaustive
  journey oracle.
- `test_query_types`: 4/4 passed.
- `test_temporal_expand`: 10/10 passed.
- `test_query_canonical`: 21/21 passed.
- `git diff --check`: passed.

## Result

Public registered-duration journey queries now defer data-dependent FIFO proof
to the execution snapshot, allowing valid temporal properties while retaining
runtime rejection of non-FIFO and arithmetic-invalid values.
