# Task 17 Acceptance Gap Fix

## Delivered

- Added `QueryCrashMatrixTest.AuthoritativeFactsCorruptionRequiresRecoveryWhileProjectionFailsClosed`.
  It uses `StorageProfile::kDebugSmallThresholds`, creates enough independent
  commits to force live authoritative facts SST files, and runs bit-flip,
  truncation, and deletion mutations in isolated databases. Reopen or the
  first canonical scan must surface a typed corruption, I/O, or recovery-
  required status.
- The same matrix builds a real `QueryProjectionStore` generation, removes its
  referenced segment, and asserts `ReadChains` returns `NotFound`, projections
  are disabled, and exactly one rebuild request is pending. This verifies the
  derived fail-closed path remains distinct from canonical corruption.
- Extended every crash-phase child/reopen assertion with a complete terminal
  query, current projection base eligibility against the authoritative cut,
  and cleanup checks for `.tmp`, `.cscratch`, and query scratch directories.

## Focused verification

```text
cmake --build build/query-debug -j2 --target test_query_crash_matrix
  [100%] Built target test_query_crash_matrix

test_query_crash_matrix --gtest_filter='QueryCrashMatrixTest.AuthoritativeFactsCorruptionRequiresRecoveryWhileProjectionFailsClosed'
  [  PASSED  ] 1 test.

test_query_crash_matrix --gtest_filter='QueryCrashMatrixTest.CrashPhaseArgumentsSurviveSigkillAndReopen'
  [  PASSED  ] 1 test.
```

The crash-phase matrix exercises all existing publication, Delta, scratch,
and cursor child phases; it completed with no stale temporary or scratch files.

## Sanitizer note

The latest sanitizer report remains truthful: macOS LeakSanitizer prints
`AddressSanitizer: detect_leaks is not supported on this platform`; the same
ASAN binary passes the focused no-leak-disabled checks 2/2. UBSAN and TSAN are
blocked by interrupted nested RocksDB builds and produced no test executable.
