# Task 22 Test Race Fix

## Implementation

Commit `91c2562` adds an optional benchmark-internal callback to
`SeedQueryBenchmarkSetupForTesting`. Production `RunQueryBenchmark` uses the
default empty callback, so Cedar's public API and defaults are unchanged. The
workload regression releases the first staged setup commit, starts a second
blocker through the callback, and waits for both setup foreground admissions
and append enqueues before releasing the bounded one-request queue. Worker
joins occur after the release-all path, and `ScopedPathCleanup` remains the
RAII cleanup owner.

## Verification

- Debug build: `cmake --build build/query-debug --target test_query_bench_options test_query_bench_workload -j2` passed.
- 30 consecutive options runs: 8/8 passed each run (240/240 cases).
- 30 consecutive workload runs: 4/4 passed each run (120/120 cases).
- Release build: `cmake --build build/query-release --target cedar_query_bench -j2` passed.
- Release smoke with bounded controls (`duration=1`, `writers=8`,
  `facts_per_txn=16`, `commit_deadline_us=5000000`, `group_queue_requests=2048`,
  `group_queue_bytes=33554432`) exited 0 and reported
  `terminal_status=OK`, `hard_gate_pass=true`, `measured_transactions=1334`,
  `measured_facts=21360`.
- `git diff --check` passed before the implementation commit.

## Scope

Only the benchmark callback seam, its deterministic workload test, and the two
Task 22 reports are part of this change. Existing unrelated dirty report files
were preserved.
