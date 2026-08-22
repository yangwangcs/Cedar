# Task 16 Report: Query Observability and Cedar File Inspection

## Status

Implemented and locally verified. User-owned `.superpowers/sdd/task-12-report.md`
was preserved and is intentionally not part of the commit.

## Delivered

- Added generation-bound `CDRSTS1` statistics with database identity, schema
  fingerprint, projection generation/base sequence, bounded HLL registers,
  histograms, top values and quantile summaries, plus CRC32C validation.
- Added `QueryStatisticsStore`, Cedar-owned `.cstats` publication and stale
  identity/generation rejection. Refresh never enters the authoritative commit
  batch or RocksDB WAL path.
- Added move-only `QueryMaintenanceHandle` and
  `Database::RefreshQueryStatistics()`. The current refresh is synchronous
  behind the handle and reports its terminal status through `Await()`.
- Added `QueryProfile`/operator actuals and cursor profile access. Recording is
  opt-in through `QueryOptions::capture_profile` and occurs at batch boundaries.
- Added bounded enum-only `QueryMetrics` counters and fixed cardinality metric
  snapshots. No query-id/text/property/parameter/value label API exists.
- Extended Cedar file roles/formats and inspection for `.cmanifest`, `.cstate`,
  `.cadj`, `.cprop`, `.cstats`, `.cscratch`; RocksDB `.sst` metadata remains a
  separate path. CLI text/JSON names include the new classes and statistics
  checksum/generation metadata.
- Fed trusted `.cstats` metadata into physical planning; unavailable or stale
  statistics remain conservative.

## Verification

```text
cmake --build build/query-debug -j2 --target test_query_observability test_cedar_files
ctest --test-dir build/query-debug --output-on-failure -R 'QueryObservability|CedarFiles'
  6/6 passed

cmake --build build/query-debug -j2 --target test_query_planner test_query_types test_projection_store
ctest --test-dir build/query-debug --output-on-failure -R 'QueryPlanner|QueryTypes|ProjectionStore'
  28/28 passed
```

## Follow-up risks

- Refresh work is currently synchronous; Cedar P4 scheduling can wrap the
  handle without changing the file format or authority boundary.
- Projection builders need to provide richer per-column sketches when exact
  data statistics are available; the current refresh derives bounded metadata
  from manifest segments and remains safe/conservative.
