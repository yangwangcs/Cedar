# Cedar WAL Group Commit Qualification Evidence

## Scope and invariants

This qualification keeps one RocksDB WAL as the only durability and recovery
authority. Cedar assembles conflict-free columnar-fact epochs, while the
embedded engine owns WAL append and sync, recovery, MemTables, VersionSet, and
MANIFEST. All production measurements retain `sync=true` and WAL enabled.
Kernel mode keeps `enable_pipelined_write=false`, `unordered_write=false`, and
`two_write_queues=false`.

## Tested commit and host

- Commit: `dd29e39459d0e5a8c235b229e36843ff86c1e44f`
- Branch: `codex/cedar-wal-group-commit`
- Compiler: Apple clang 21.0.0 (clang-2100.1.1.101)
- Host: Darwin 25.5.0, arm64
- Engine: embedded under `src/engine/rocksdb` in the Cedar source tree.

## Correctness gates

The following were run from this commit:

```sh
cmake --build build-main-debug -j2
ctest --test-dir build-main-debug --output-on-failure
cmake --build build-main-asan --target test_kernel_commit \
  test_kernel_bounded_benchmark test_recovery_crash_matrix -j2
ctest --test-dir build-main-asan \
  -R 'KernelCommit|KernelBoundedBenchmark|RecoveryCrash' --output-on-failure
```

Results:

- Debug: 452/452 passed in 56.34 seconds.
- ASAN focused commit/recovery/benchmark gate: 22/22 passed in 6.02 seconds.

The added regression coverage verifies all three boundaries below:

- A post-write indeterminate concurrent submission resolves every member whose
  async handle crossed Cedar's WAL-durable callback after reopen. A request
  that did not cross that callback is not reported committed. Scheduler timing
  is deliberately not used to assert an exact physical group size.
- Two conflicting writes preserve exactly-one-winner conflict behavior under
  high fan-in while independent writes still form multi-request groups.
- Close during an N+1 successor candidate discards it with `kShutdown`,
  terminates every submitted handle, and permits reopen.

The regression also found and fixed metric attribution: `durably_accepted` now
counts the unique transactions only after the WAL-durable callback, rather than
incorrectly requiring the later publication result to be OK. This matters for
a post-write indeterminate result, where data is durable but the caller must
reopen to resolve it.

## Historical fan-in matrix

Raw data: `/private/tmp/cedar-group-matrix-20260820/`.

This matrix was collected before `dd29e39`, so it is evidence of the group
commit implementation shape, not a matched performance measurement of the
metric-only fix in this commit. Each row used `property-put`, 30 seconds,
`verify-reopen=false`, and has zero writer, background, maintenance,
write-stopped, unexplained-autonomous-job, and pending-compaction errors.

| Clients | Ops/s | Transactions/WAL sync | Group p50/p95/max |
| ---: | ---: | ---: | --- |
| 2 | 666.331 | 2.000 | 2 / 2 / 2 |
| 4 | 815.017 | 2.040 | 2 / 2 / 4 |
| 8 | 1539.460 | 3.930 | 4 / 8 / 8 |
| 16 | 3215.680 | 7.801 | 8 / 16 / 16 |
| 32 | 6664.720 | 16.035 | 16 / 32 / 32 |
| 64 | 11692.600 | 32.138 | 32 / 64 / 64 |
| 128 | 17570.500 | 63.922 | 64 / 128 / 128 |

Every row is `warm_not_sustained`. The low 2-client fill is intentional:
blocking clients provide only about two concurrent transactions to amortize a
sync. It is not a peak-throughput result.

## Current read controls

Raw CSVs: `/private/tmp/cedar-read-controls-20260820-dd29e39/`.

Each ran for 30 seconds with `verify-reopen=true`, one shared 4,096-fact seed,
and zero writer/background/maintenance/write-stopped/unexplained-autonomous
errors. They are current absolute controls, not an A/B regression comparison:
there is no matched pre-change sample on the same host, seed, data size, and
binary configuration.

| Workload | Ops/s | Reopen | Qualification |
| --- | ---: | --- | --- |
| mixed-90-write-10-point-read | 344.224 | passed | warm_not_sustained |
| point-read | 868040.000 | passed | warm_not_sustained |
| projected-event-scan | 929.093 | passed | warm_not_sustained |

## Current write spot check

Raw CSV: `/private/tmp/cedar-read-controls-20260820-dd29e39/current-64.csv`.

The current production-code commit (`dd29e39`) also completed a 64-client,
30-second `property-put` run with reopen verification: 9,210.110 ops/s,
32.066 transactions per WAL sync, group p50/p95/max of 32/64/64, and zero
writer/background/maintenance/write-stopped/unexplained-autonomous errors.
It is `warm_not_sustained`. Its raw value is not compared against the older
matrix because the runs were not a matched A/B experiment.

## Sustained Release status

Not qualified. Two prior 64-client, 1,800-second Release runs were terminated
with exit code 137 after approximately 13-14 minutes before emitting a valid
CSV. The second raw directory is
`/private/tmp/cedar-throughput-sustained-detached.kKIEbU/`; its `result.csv` is
empty and must not be interpreted as a result. The database was approximately
238 MiB, temporary disk was available, and captured RocksDB logs showed normal
write-buffer flushes with zero pending compaction, but that does not prove the
termination source.

Therefore no sustained throughput, sustained reopen, or no-read-regression
claim is made. A valid future sustained result must use the same commit,
binary, host, profile, client count, group limits, seed/database size, and
reopen flag, run the full 1,800 seconds, and emit a qualified CSV.
