# Task 17 Fixes Report

## Scope

This follow-up closes the Task 17 review findings without touching the dirty
Task 12 report or any Task 18 benchmark files. The production-independent
oracle now derives edge visibility from corrected identity and edge-state
histories, intersects half-open intervals, enumerates bounded simple paths,
and sorts EarliestArrival, LatestDeparture, and FastestDuration witnesses by
their objective, hop count, and lexicographic EdgeRef sequence. Its event-log
serialization is stable and replayable.

The differential suite now includes real Cedar canonical execution in the
interactive, analytical, and auto lanes, plus temporal interval, objective,
serialization, and debug-profile lifecycle regressions. The debug profile
selector remains public only as `StorageProfile::kDebugSmallThresholds`; its
capacity values live in an internal kernel header. A real debug database test
performs 256 commits, reaches a snapshot, and runs Vacuum.

Cursor cancel/close lifecycle hooks were added to the shared execution state.
The subprocess SIGKILL matrix covers projection segment, manifest, CURRENT,
Delta enqueue, scratch, and cursor lifecycle boundaries. Close/Vacuum is
repeated 100 times and all focused tests assert clean reopen and no temporary
files.

## Focused verification

```text
cmake --build build/query-debug -j2 --target test_query_differential test_query_crash_matrix
build/query-debug/tests/test_query_differential --gtest_filter='*Oracle*'
  [  PASSED  ] 5 tests.
build/query-debug/tests/test_query_differential --gtest_filter='*Lanes*'
  [  PASSED  ] 1 test.
build/query-debug/tests/test_query_differential --gtest_filter='*DebugProfile*'
  [  PASSED  ] 1 test.
build/query-debug/tests/test_query_crash_matrix --gtest_filter='QueryCrashMatrixTest.CrashPhaseArgumentsSurviveSigkillAndReopen' --gtest_break_on_failure
  [  PASSED  ] 1 test (25 SIGKILL phases).
build/query-debug/tests/test_query_crash_matrix --gtest_filter='QueryCrashMatrixTest.CloseAndVacuumRemainIdempotentAcrossRepeatedPins:QueryCrashMatrixTest.ProjectionBitFlipDeletionAndTruncationFailClosed'
  [  PASSED  ] 2 tests.
```

ASAN/LSAN, UBSAN, and TSAN configure/build/test commands are retained in the
Task 17 brief. Their exact status is recorded here after the toolchain runs;
configuration or resource failures are reported verbatim rather than claimed
as sanitizer passes.
