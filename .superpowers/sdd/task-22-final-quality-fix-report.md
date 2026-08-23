# Task 22 Final Quality Fix

## Status

Implemented the final bounded-admission regression redesign. The workload test
now stages three blocker commits through the existing append collection hook,
gates both setup-thread foreground admissions, and keeps each setup commit
under a one-request queue while the configured 500,000 us deadline is active.
Collection and admission waits are bounded, and the final release-all path
executes before joining every worker thread. `ScopedPathCleanup` remains the
RAII owner of the temporary database path.

Mutation checks confirmed coverage: temporarily removing the deadline from
`SeedGraph` failed with `ResourceExhausted: commit: append queue is full`, and
temporarily removing it from `SeedBenchmarkScore` failed with the same bounded
admission error. Both callsites were restored.

## Commits

`1feebfd test: cover both task 22 setup admissions`

## Tests

- `build/query-debug/tests/test_query_bench_options --gtest_color=no`: 8/8 passed.
- `build/query-debug/tests/test_query_bench_workload --gtest_color=no`: 4/4 passed.
- Mutation probes for each setup deadline: both failed as expected.
- Release smoke (`cedar_query_bench`, 1 s, 8 writers, 16 facts/transaction,
  5,000,000 us deadline, 2,048 requests, 32 MiB): exit 0; CSV/JSON reported
  `terminal_status=OK`, `hard_gate_pass=true`, `measured_transactions=1360`,
  and `measured_facts=21776`.

## Concerns

The release smoke output and database are retained under
`/tmp/cedar-task22-final-quality.olxprs` for this run only. Historical
Task 22 baseline artifacts remain absent and are not used as acceptance
evidence. The test's short timed holds are bounded at 20 ms, well below the
500 ms setup deadline, while still forcing each setup call to encounter a full
append queue.
