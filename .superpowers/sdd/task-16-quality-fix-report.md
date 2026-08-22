# Task 16 Quality Fix Report

## Changes

- `QueryStatisticsStore::Refresh` now serializes publication with a Cedar-owned
  mutex and uses unique temporary paths per refresh/thread.
- Refresh decodes every manifest-referenced projection segment and derives rows,
  pages, bytes, interval and adjacency-edge counts from decoded data. HLL
  registers, bounded numeric histograms, top values, fanout and interval-length
  quantiles are populated deterministically. A segment read/checksum/decode
  failure keeps the snapshot published but marks `complete=false`, so the
  planner remains conservative.
- Query batch profiles no longer relabel planner estimates as runtime actuals:
  decoded bytes remain batch-accounted; physical bytes, pages and interval
  fragments are zero when decoder-level counters are unavailable.
- Cedar file inspection now sets and reports `query_file.available` for JSON
  and text output, distinguishing decode failure from a valid file with an
  invalid checksum.
- Added decoded-segment, concurrent/repeated-refresh regression coverage.

## Verification

```text
cmake --build build/query-debug -j2 --target \
  test_query_observability test_cedar_files test_query_planner \
  test_query_types test_coexisting_path                         PASS
ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryObservability|CedarFiles|QueryPlanner|QueryTypes|CoexistingPath' \
  35/35 passed
git diff --check                                                PASS
```

Commit: `bbad064`.
