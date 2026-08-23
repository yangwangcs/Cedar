# Task 22 Final Quality Fix

## Status

Commit `91c2562` makes the bounded-admission regression deterministic. The
benchmark-only callback runs between graph and score setup commits; the test
uses it to stage the second blocker after the first setup admission and before
the score admission. All observer waits and teardown remain bounded, and
`ScopedPathCleanup` owns the temporary database path.

## Tests

- 30 consecutive runs of `build/query-debug/tests/test_query_bench_options --gtest_color=no`: 8/8 passed on every run (240/240 test cases).
- 30 consecutive runs of `build/query-debug/tests/test_query_bench_workload --gtest_color=no`: 4/4 passed on every run (120/120 test cases).
- `cmake --build build/query-release --target cedar_query_bench -j2`: passed.
- Release smoke (`cedar_query_bench`, 1 s, 8 writers, 16 facts/transaction,
  5,000,000 us deadline, 2,048 requests, 32 MiB): exit 0; CSV/JSON reported
  `terminal_status=OK`, `hard_gate_pass=true`, `measured_transactions=1334`,
  and `measured_facts=21360`.

## Concerns

The smoke output and database are retained under
`/tmp/cedar-task22-race-smoke.VYL6z2` for this run only. Historical Task 22
baseline artifacts remain absent and are not used as acceptance evidence.
