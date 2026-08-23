# Task 22 Fix: Setup admission propagation

## Status

Implemented. `SeedGraph` and `SeedBenchmarkScore` now receive
`QueryBenchmarkOptions::commit_deadline_us` and pass it in
`TransactionOptions`, matching the existing fact-seed and timed-writer paths.
Public Cedar defaults are unchanged. The original Task 22 report now contains
absolute executable, database, CSV, and JSON paths for each probe.

## Verification

Focused tests passed:

```text
/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-debug/tests/test_query_bench_options
8 tests passed; exit status 0

/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-debug/tests/test_query_bench_workload
4 tests passed; exit status 0
```

The workload suite includes `BoundedAdmissionCoversAllSetupWrites`, which
executes the setup and timed-write call path with a nonzero deadline.

Release mixed validation (exit status `0`):

```text
/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-release/cedar_query_bench --path=/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-release/evidence/task-22-fix/mixed-5m-database --operation=state-at --projection-state=canonical-only --projection-work=paused --degree=1 --selectivity-percent=1 --readers=1 --cache-state=cold --writers=32 --facts-per-txn=1024 --duration-seconds=1 --commit-deadline-us=5000000 --group-queue-requests=2048 --group-queue-bytes=33554432 > /Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-release/evidence/task-22-fix/mixed-5m.csv 2> /Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-release/evidence/task-22-fix/mixed-5m.json
```

The JSON artifact reports `terminal_status="OK"`, `hard_gate_pass=true`,
`transactions=86`, `facts=87057`, and `measured_transactions=84`.

## Concerns

The 500000-us, current-bounds variant on this host exited 1 with
`ResourceExhausted: commit: append admission deadline expired`; its artifacts
are under `/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-release/evidence/task-22-fix/mixed-database`,
`/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-release/evidence/task-22-fix/mixed.csv`, and
`/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query/build/query-release/evidence/task-22-fix/mixed.json`.
This is host/load sensitivity, not a setup propagation failure. No long
campaign was run.
