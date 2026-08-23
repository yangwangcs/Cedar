# Review Findings Fix Report

## Changes

- Canonical temporal sources now use a family/property-scoped columnar scan.
  The storage boundary enumerates matching home partitions from fact keys only,
  then runs the existing exact `EventColumnarScan` for each partition. This
  avoids decoding unrelated properties while preserving multi-partition reads.
- Canonical multi-property bindings now charge every string/binary value in
  `property_values` for retained row memory, read-byte accounting, and output
  materialization/output-byte accounting. Overflow is reported as resource
  exhaustion.
- Registered journey duration FIFO validation clips each duration history to
  the finite intersection of the edge effective interval and request interval,
  so positive durations in an unbounded history are not tested at `UINT64_MAX`
  when the request is finite. Reverse and forward searches use the same rule.

## Regression Coverage

- `QueryCanonicalTest.ReadsCanonicalAndPropertiesAcrossHomePartitions`
- `QueryCanonicalTest.ChargesAllCanonicalStringPropertyValuesToOutputBudget`
- `TemporalJourneyTest.PublicRegisteredDurationClipsUnboundedEdgeToFiniteQueryInterval`
- Existing registered-duration non-FIFO rejection and canonical temporal suites

## Verification

```text
cmake --build build/query-debug -j2 --target test_query_canonical test_temporal_journey
ctest --test-dir build/query-debug --output-on-failure -R \
  'QueryCanonicalTest.ReadsCanonicalAndPropertiesAcrossHomePartitions|ChargesAllCanonicalStringPropertyValuesToOutputBudget|PublicRegisteredDurationNonFifoRejectedAtExecution|PublicRegisteredDurationClipsUnboundedEdgeToFiniteQueryInterval'
```

All four selected tests passed. The broader query-focused run passed 59/59
tests after rebuilding the test binaries.
