# Task 18 Follow-up Report

Status: implemented with documented bounded equivalents.

## Implemented

- `facts_per_txn` now controls one real Cedar `Transaction` containing that
  many assertions; the timed write loop uses the configured writer count and
  duration.
- Each write records a measured transaction latency sample. Query readers run
  real snapshot scans for every typed operation entry (event/history/change
  operations use event scans; the remaining public-API matrix entries use a
  bounded state scan because the public benchmark surface has no generic
  operation dispatcher).
- CSV/JSON now include measured query and write p50/p95/p99, writer and batch
  controls, authoritative/derived/scratch/total storage bytes from
  `InspectStorageFiles`, projection-work state, and hard-gate classification.
- `projection-work=active` starts Cedar query maintenance through the existing
  `RefreshQueryStatistics` API; paused mode does not start it. No projection
  builder API is exposed by the current public Database interface, so active
  mode is a real Cedar maintenance workload rather than a fabricated projection
  segment.
- Reopen verification remains enabled and checks an authoritative fact after
  the timed run.

## Deliberate residuals

The current public API does not expose a benchmark-only projection builder,
projection pause/resume controller, or a generic operation execution hook.
Consequently, projection-state labels remain workload metadata and the graph /
journey operation entries execute their bounded canonical state source. They
are not silently skipped; the limitation is explicit here and in code comments.

## Verification

```text
build/query-debug/cedar_query_bench ... --writers=2 --facts-per-txn=16
  -> 356 timed transactions, 5696 facts, 8 query samples, reopen_verified=true

ctest --test-dir build/query-debug --output-on-failure -R 'QueryBench|KernelBench'
  -> 15/15 passed

cmake -DCEDAR_BENCHMARK=$PWD/build/query-debug/cedar_query_bench \
  -P tests/performance/test_query_benchmark_csv.cmake
  -> passed
```

The pre-existing `.superpowers/sdd/task-12-report.md` dirty change was not
modified or staged.
