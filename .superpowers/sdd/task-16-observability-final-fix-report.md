# Task 16 Observability Final Fix

## Changes

- Added `StatisticsReference` to generation manifests (manifest format v2,
  while retaining decoding of v1 manifests without a reference).
- `QueryStatisticsStore::Refresh` now writes and syncs the immutable cstats
  payload, publishes the linked generation manifest via temp+fsync+rename,
  then replaces `CSTATS-CURRENT` with generation/base/payload/manifest checksums.
  `Load` requires all reference fields and both checksums to agree, so crashes
  between publication phases fall back to no statistics.
- Profile actuals are recorded at vector-batch boundaries only.  The default
  synthetic operator is gone; the selected physical operator receives rows,
  batches, bytes, pages, interval fragments, wall time, thread CPU time, and
  first-result latency when capture is enabled.
- Decoded and physical byte estimates are computed for every query, including
  `capture_profile=false`, and are exported through global `QueryMetrics`.

## Verification

```text
cmake --build build/query-debug -j2 --target test_query_observability PASS
ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryObservability|CedarFiles|QueryPlanner|QueryTypes|ProjectionStore|CoexistingPath' 47/47 PASS
git diff --check PASS
```
