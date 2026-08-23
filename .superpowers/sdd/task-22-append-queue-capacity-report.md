# Task 22: Append admission under mixed load

## Scope

The query benchmark's 32-writer x 1024-facts/transaction workload was tested
without changing Cedar's WAL, recovery, or production admission implementation.
The benchmark now exposes three workload-only controls:

- `--commit-deadline-us` (default `0`, preserving public immediate admission)
- `--group-queue-requests` (default `1024`)
- `--group-queue-bytes` (default `16777216` / 16 MiB)

The selected values are emitted in both CSV and JSON metadata.

## Reproduction and one-variable probes

All runs used `state-at`, `readers=1`, `degree=1`, `facts-per-txn=1024`,
`writers=32`, release build, and `reopen-verify=false` unless noted.

### Baseline: deadline 0, current bounds, duration 2 s

Command:

```text
cedar_query_bench --duration-seconds=2 --writers=32 --facts-per-txn=1024
```

Terminal status: `ResourceExhausted: commit: append queue is full` (process
exit 1). The run recorded 127 measured transactions / 131072 measured facts,
query operations 11, query p50/p95/p99 188993/191724/191724 us,
write p50/p95/p99 445842/554302/1605901 us, WAL-sync p99 5000 us,
end-to-end p99 549125 us, group-fill p50 2, and `metrics_complete=true`.

### Variable 1: bounded deadline 500000 us, current bounds

At duration 2 s this completed with terminal status `OK` and hard gate pass.
It recorded 304 measured transactions / 312320 measured facts. This isolates
the admission wait from queue capacity and removes the queue-full failure.

### Variable 2: larger bounds, deadline 0

The explicit 4096-request / 128 MiB probe admitted a backlog and continued
draining well beyond the 2-second measurement window; it was stopped without a
terminal CSV result. This is not evidence of sustained capability and shows
that larger bounds alone can defer failure while increasing drain cost.

### Combined short validation

With `5000000us`, 2048 requests, and 32 MiB, a 1-second run completed with
terminal status `OK`, hard gate pass, and 100 measured transactions / 103424
facts. This confirms the metadata path and the bounded benchmark contract.

## Implementation and tests

`QueryBenchmarkOptions` carries benchmark-only admission settings. Every query
workload seed/write transaction receives the selected deadline, and the
benchmark's `DatabaseOptions` receives the selected Cedar queue bounds. Public
`TransactionOptions` and `DatabaseOptions` defaults are unchanged.

Focused tests:

```text
build/query-debug/tests/test_query_bench_options
build/query-debug/tests/test_query_bench_workload
```

Both passed (8 option tests and 3 workload tests). A release mixed benchmark
with bounded admission also passed as described above.

## Concerns

The queue-only zero-deadline probe was intentionally stopped after it exceeded
the short-run budget; no sustained throughput claim is made. The default
benchmark remains a faithful zero-deadline reproduction and therefore can still
report `append queue is full` under this workload. Callers selecting a bounded
deadline must choose a value appropriate to their benchmark duration and host.
