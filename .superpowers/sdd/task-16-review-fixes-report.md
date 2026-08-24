# Task 16 Observability Review Fixes

## Result

DONE. The review findings are addressed in commit `fix: close task 16 observability review findings`.

## Fixes

- Added the shared `CQUERY-PUBLISH.lock` publication lock to projection and statistics generation publication. `Refresh` holds it from current-generation validation through `CSTATS-CURRENT` replacement, while `QueryProjectionStore::Build` holds the same lock for projection publication. This removes the validation/publish TOCTOU across processes.
- `QueryStatisticsStore::Refresh` now validates the supplied manifest with `ValidateProjectionManifest`, rejects foreign database identities, and rejects schema fingerprints not bound by the manifest before creating any artifact.
- Added fixed-size enum counters and bounded log-scale histograms for admission, latency, projection hit/fallback, bytes, memory/scratch, worker/I/O wait, Delta lag, projection health, adjacency pruning, and label dominance. No string-label or query-identifier API was added.
- Scratch inspection now reports an empty coverage string; query id and payload length are not treated as canonical coverage.

## Verification

```
cmake --build build/query-debug -j2 --target test_query_observability test_cedar_files
build/query-debug/tests/test_query_observability --gtest_color=no
build/query-debug/tests/test_cedar_files --gtest_color=no
```

Observed output: 10 QueryObservability tests passed (including publication-lock and manifest identity/schema regressions), and 3 CedarFiles tests passed.
