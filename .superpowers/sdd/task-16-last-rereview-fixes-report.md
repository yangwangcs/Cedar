# Task 16 Last Re-review Fixes

Result: DONE

## Changes

- Extended public `cedar::QueryMetricsSnapshot` and `Database::SampleQueryMetrics()` with admission, projection, projection health, adjacency pruning, label dominance, fixed latency/wait/lag histograms, memory, and scratch dimensions.
- Made every `PROJECTION-CURRENT` publication path use `CQUERY-PUBLISH.lock`.
- Made projection `Build` re-read durable `PROJECTION-CURRENT` under the lock and reject stale or same-generation publication.
- Made `RetireBefore` validate durable `CURRENT` still names the local generation before publishing generation zero; stale-process retirement now returns `Conflict`.
- Added public snapshot contract coverage and a stale durable `CURRENT` retirement regression.

## Verification

Command:

```text
cmake --build build/query-debug -j2 --target test_projection_store test_query_observability
```

Output:

```text
[100%] Built target test_projection_store
[100%] Built target test_query_observability
```

Command:

```text
ctest --test-dir build/query-debug --output-on-failure -R 'ProjectionStore|QueryObservability|QueryLifecycle'
```

Output:

```text
100% tests passed, 0 tests failed out of 31
Total Test time (real) =   0.91 sec
```

## Commit

Commit message:

```text
fix: finish task 16 publication and metrics contracts
```

Paths committed:

```text
.superpowers/sdd/task-16-last-rereview-fixes-report.md
include/cedar/database.h
src/kernel/database.cc
src/query/projection/projection_store.cc
tests/query/test_projection_store.cc
tests/query/test_query_observability.cc
```
