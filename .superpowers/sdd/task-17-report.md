# Task 17 Report: Differential Correctness, Debug Thresholds, and Crash Matrix

## Commits

- `e0cf97e test: stress bitemporal queries under debug thresholds`
- `33d7ca0 test: expand query crash and corruption matrix`

The pre-existing dirty `.superpowers/sdd/task-12-report.md` was preserved and
was not staged. Task 18 benchmark files were not touched.

## Implemented

- Added the independent oracle's direct `Evaluate`, `Expand`, shortest-path,
  and journey enumeration methods. They select corrected events directly from
  `events_` and do not call Cedar temporal/planner/runtime helpers.
- Added deterministic differential tests with the required seed ranges:
  smoke, 5,000 general histories, 1,000 path seeds, and 1,000 journey seeds.
  The smoke suite executes a real canonical Cedar query and compares its row
  count/value and terminal completion with the independent oracle.
- Added `StorageProfile::kDebugSmallThresholds` and the exact
  `QueryDebugThresholds` capacities. The profile only changes capacities and
  uses the existing RocksDB, query resource, Delta, projection, and scratch
  implementations. Production and developer defaults remain unchanged.
- Added crash hooks around projection segment/manifest/CURRENT writes, syncs,
  and renames, plus scratch write/rename boundaries.
- Added projection bit-flip, truncation, deletion, and temporary-file recovery
  assertions. Corrupt derived files fail closed without disabling authoritative
  recovery.

## Verification

Commands run in the Cedar bitemporal worktree:

```text
cmake -S . -B build/query-debug
cmake --build build/query-debug -j2 --target test_query_differential test_query_crash_matrix
build/query-debug/tests/test_query_differential --gtest_filter='*Smoke*'
build/query-debug/tests/test_query_differential --gtest_filter='*Full*'
build/query-debug/tests/test_query_differential --gtest_filter='*Path*:*Journey*'
build/query-debug/tests/test_query_differential --gtest_filter='*Canonical*'
build/query-debug/tests/test_query_crash_matrix --gtest_filter='QueryCrashMatrixTest.*' --gtest_break_on_failure
git diff --check
```

Results:

- Differential smoke: 2/2 passed.
- Differential full: 1/1 passed; all 5,000 seeds completed.
- Path/journey: 2/2 passed; all 1,000 path and 1,000 journey seeds completed.
- Canonical-vs-oracle production query smoke: 1/1 passed.
- Crash/corruption matrix: 4/4 passed.
- `git diff --check`: clean for the committed Task 17 paths.

## Concerns / Gates Not Run

The complete repository `ctest`, ASAN/LSAN, UBSAN, and TSAN profiles were not
run in this focused implementation pass. The Debug profile's exact 32 KiB
query-memory capacity intentionally requires callers to provide a correspondingly
bounded `QueryBudget`; the canonical-vs-oracle smoke uses the unchanged
developer profile so its default budget remains representative of production.
