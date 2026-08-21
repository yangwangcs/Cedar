# Task 14 Review Fix Report

## Fixed

- Journey target discovery now uses `ExpandSpec` direction and edge type and
  runs `KHopExpand` for the declared hop bound, excluding depth-zero labels.
  Journey traversal orientation supports outbound, inbound, and both modes.
- Duration properties are included in the prepared-query schema fingerprint and
  must be edge-owned `int32`, `int64`, or `timestamp64` values. Callback duration
  functions are checked for a bounded FIFO witness before traversal.
- Latest and fastest reject unbounded or excessively wide candidate intervals,
  propagate all errors except `NotFound`, and receive the public graph-label
  budget as their journey label limit.
- Duration lookup merges snapshot events with the bound `QueryDeltaView`, so
  newly published duration facts are evaluated consistently with visibility.
- Journey nested columns use their per-array offsets without a misleading
  duplicate `row_offsets` field; the unused edge comparator was removed.

## Verification

```text
cmake --build build/query-debug -j2 --target test_temporal_journey test_coexisting_path test_query_types
ctest --test-dir build/query-debug --output-on-failure -R 'TemporalJourney|CoexistingPath|QueryTypes'
```

Result: all 21 tests passed.
