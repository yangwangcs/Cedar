# Task 22 Quality Fix

## Status

Implemented the setup propagation regression fix and temporary-path cleanup.
The graph and score setup now run through
`SeedQueryBenchmarkSetupForTesting`, which calls the same setup helpers used by
`RunQueryBenchmark` with the selected commit deadline. The focused regression
holds an existing queued commit at the append collection hook with a
single-request queue, waits until setup reaches foreground admission, and
then releases the worker. A bounded setup deadline succeeds under this
pressure; the pre-fix zero-deadline setup would be rejected as queue full.

`ScopedPathCleanup` removes workload test database paths from the destructor,
including assertion/failure exits.

The original Task 22 probe artifacts remain absent. The append-capacity report
now labels those historical values as unretained observations rather than
retained artifacts or acceptance evidence.

## Commits

Committed in the current worktree as `test: strengthen task 22 setup admission regression`.

## Tests

- `build/query-debug/tests/test_query_bench_options --gtest_color=no`: 8/8 passed.
- `build/query-debug/tests/test_query_bench_workload --gtest_color=no`: 4/4 passed.
- Release build: `cmake --build build/query-release --target cedar_query_bench -j2` passed.
- Short release bounded run (1 s, 8 writers, 16 facts/transaction, 5,000,000 us deadline, 2,048 requests, 32 MiB): exit `0`; CSV/JSON reported `terminal_status=OK`, `hard_gate_pass=true`, `measured_transactions=1276`, and `measured_facts=20432`.

## Concerns

The original baseline/deadline/combined CSV, JSON, and database files are not
present under `build/query-release/evidence/task-22-original`; the corrected
capacity report does not use them as independently checkable acceptance
evidence. The short release run used `/tmp/cedar-query-quality-fix-release`
and its output files for verification.
