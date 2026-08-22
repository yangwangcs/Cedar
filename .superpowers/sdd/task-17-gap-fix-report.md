# Task 17 Gap Fix Report

## Status

DONE_WITH_CONCERNS. The Important review gaps were addressed within existing
Cedar APIs. The pre-existing dirty `.superpowers/sdd/task-12-report.md` was
preserved and was not staged.

## Delivered

- Differential topology tests now compare projection base, short Delta, and
  long Delta materialized intervals and values against `BitemporalFactOracle`.
- Added an eight-seed bounded randomized real Cedar path/journey campaign;
  each seed compares production execution with the independent oracle.
- Wired debug `memtable_bytes`, `projection_page_bytes`,
  `delta_lag_soft_commits`, and `manifest_commits_per_generation` into their
  owning components. Added Delta soft-lag observability and projection
  generation rollover/reader pin/cleanup assertions.
- Crash child now performs concurrent bounded commits and a query before the
  selected fault phase.

## Verification

```text
cmake --build build/query-debug -j2 --target test_query_differential test_query_delta test_projection_store test_query_crash_matrix
  passed (all four targets built)
test_query_differential --gtest_filter='QueryDifferentialTest.BoundedRandomizedProductionPathJourneyMatchOracle:QueryDifferentialTest.ProjectionAndDeltaTopologiesRemainSnapshotCorrect'
  2/2 passed
test_query_delta --gtest_filter='QueryDeltaTest.SoftLagThresholdIsObservableBeforeHardRetirement'
  1/1 passed
test_projection_store --gtest_filter='ProjectionStoreTest.DebugBoundsPreserveRolloverCoverageAndCleanup'
  1/1 passed
git diff --check
  passed
```

## Unresolved concerns

The full 25-phase crash subprocess was not rerun after the final bounded-loop
edit in this pass; the target compiled successfully. Existing sanitizer
limitations remain unchanged: macOS LSAN reports leak detection unsupported,
while UBSAN/TSAN nested RocksDB builds remain interrupted; no sanitizer pass is
claimed. Production has no public projection-builder or Delta injection API,
so randomized coverage is intentionally bounded to real commit/query APIs.
