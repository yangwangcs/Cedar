# Task 17 Final Fixes Report

## DONE

- `tests/query/test_query_differential.cc` now normalizes every emitted vertex
  row, compares the complete ordered result with the independent oracle, and
  requires a clean terminal (`complete=true`) for canonical, interactive,
  analytical, and auto execution.
- Budget and CPU deadline runs compare the emitted rows against the exact
  prefix of the eight-row snapshot oracle. Cancellation is checked by calling
  `Next()` after `Cancel()`, requiring a query-cancelled status, an empty
  prefix, and `complete=false`.
- A real Cedar `CoexistingShortestPath` and `EarliestArrival` execution is
  compared field-for-field with the independent oracle. The fixture includes
  cross-partition vertices, a two-hop path, half-open edge state, and a cycle
  shape in the generated histories.
- The deterministic generator now includes same-time corrections, Missing
  values, schema epochs, empty/touching intervals, self-loop, parallel,
  cross-partition, and cycle edges. Serialization remains stable and includes
  the complete event log for replay.

Focused verification:

```text
cmake --build build/query-debug -j2 --target test_query_differential
  [100%] Built target test_query_differential
test_query_differential --gtest_filter='QueryDifferentialTest.ProductionPathAndJourneyMatchIndependentOracle:QueryDifferentialTest.FaultBudgetsAndCancellationOnlyEmitSnapshotPrefixes:QueryDifferentialTest.CanonicalMatchesIndependentOracleAcrossExecutionLanes'
  [  PASSED  ] 3 tests.
```

## CONCERNS

The existing public/internal interfaces do not expose projection-generation
selection or a production Delta injection point from this test binary, so
projection-at-base, short/long Delta, partial-coverage, and restart-rebuilt
topologies remain covered by their owning projection/Delta suites rather than
silently skipped here. DebugSmall lifecycle inspection still needs exact
flush/compaction counters; this patch does not invent tautological counters.

Sanitizer gates are run separately and their exact configure/build/test output
must be appended here before claiming a pass. A configuration success alone is
not a sanitizer result.

Observed sanitizer gate output (22 Aug 2026):

```text
ASAN/LSAN configure: failed during CMake generation:
CMake Error at tests/CMakeLists.txt:346 (add_test):
  Error evaluating generator expression:
    $<TARGET_FILE:cedar_kernel_bench>
  No target "cedar_kernel_bench"
Generate step failed.  Build files cannot be regenerated correctly.

UBSAN configure/build: CMake reached the nested RocksDB build, then the run
was interrupted while compiling `db/blob/blob_file_reader.cc`; no Cedar test
executable was produced.

TSAN configure/build: CMake reached the nested RocksDB build, then the run
was interrupted while compiling `db/blob/blob_file_reader.cc`; no Cedar test
executable was produced.
```

ASAN/LSAN, UBSAN, and TSAN are therefore **blocked**, not passed; no
sanitizer query filter produced an executable or test result.

Follow-up: `tests/CMakeLists.txt` now guards `KernelBenchmarkCsvContract` with
`if(TARGET cedar_kernel_bench)`. ASAN/LSAN reconfiguration then succeeded and
`test_query_differential` built. The required LSAN invocation still fails on
this macOS toolchain with the verbatim output:

```text
==48282==AddressSanitizer: detect_leaks is not supported on this platform.
```

The same ASAN executable without `detect_leaks=1` passed the two focused tests
(2/2). UBSAN and TSAN remained blocked in their fresh nested RocksDB builds
after interruption; no executable was available for either query filter.
