# Cedar Query System: Local Performance Results

Date: 2026-08-24 (Asia/Shanghai)

This report records measurements produced on the Cedar development machine. It
is not a hardware-independent benchmark claim. Results are tied to the source,
build, workload, and admission parameters below.

## Test Identity

- Worktree: `/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query`
- Branch: `codex/cedar-bitemporal-query-execution`
- Source: `ce554a6` (Cedar bitemporal-query implementation)
- Host: Darwin arm64, Apple clang 21.0.0, CMake 4.2.1
- Build: `build/query-release`
- Storage model: one Cedar-controlled RocksDB WAL, authoritative CedarParquet
  facts, Cedar projection/QueryDelta layers
- WAL admission: `commit_deadline_us=5000000`,
  `group_queue_requests=2048`, `group_queue_bytes=33554432`

## Write Performance

Release calibration used one writer, one reader, paused projection work, and a
two-second timed window per batch size. `facts/s` is the primary metric because
one transaction can contain many facts; `txn/s` is derived as `facts/s /
facts_per_txn`.

| Facts per transaction | Facts/s | Approx. transactions/s |
|---:|---:|---:|
| 1 | 366 | 366 |
| 4 | 1,380 | 345 |
| 8 | 2,580 | 323 |
| 16 | 4,555 | 285 |
| 32 | 8,355 | 261 |
| 64 | 15,208 | 238 |
| 128 | 25,022 | 195 |
| 256 | 41,404 | 162 |
| 512 | 73,495 | 144 |
| 1024 | **111,396** | **109** |
| 2048 | 108,704 | 53 |

The local batch-size turning point is approximately `1024 facts/transaction`.
Throughput rises as a WAL sync amortizes more facts, then starts to flatten and
decline when transaction assembly and processing cost dominate.

Focused five-repeat write measurements:

| Workload | Average facts/s | Average end-to-end p99 |
|---|---:|---:|
| Idle, 16 facts, 1 writer | 5,018 | 5 ms |
| Idle, 16 facts, 8 writers | 19,704 | 13 ms |
| Idle, 64 facts, 1 writer | 15,761 | 7 ms |
| Idle, 64 facts, 8 writers | 49,999 | 25 ms |
| Active projection, 16 facts, 1 writer | 4,773 | 5 ms |
| Active projection, 16 facts, 8 writers | 18,829 | 16 ms |
| Active projection, 64 facts, 1 writer | 14,856 | 6 ms |
| Active projection, 64 facts, 8 writers | 65,301 | 19 ms |

WAL-sync p99 was normally `5 ms` in these focused Release cases. The
8-writer/64-fact active result is higher than its idle counterpart and should
be treated as run-to-run variance until a larger paired sample is collected;
it is not used as a universal active-maintenance overhead claim.

## Read Performance

The reduced cold and warm matrices each contain 270 passing cases. They cover
15 query operators, five projection states, reader counts 1 and 8, multiple
degrees/selectivities, reopen verification, and space accounting. The values
below are averages across the matrix for each operator, so they are useful for
relative comparison rather than a single fixed-query SLA.

| Operator | Cold qps | Warm qps | Cold p99 | Warm p99 |
|---|---:|---:|---:|---:|
| `state-at` | 36.6 | 37.4 | 229 ms | 232 ms |
| `history` | 81.7 | 85.7 | 126 ms | 116 ms |
| `events` | 77.3 | 82.0 | 126 ms | 117 ms |
| `property-filter` | 53.6 | 57.2 | 123 ms | 98 ms |
| `expand-out` | 195.6 | 201.5 | 47 ms | 43 ms |
| `expand-in` | 134.3 | 136.7 | 92 ms | 89 ms |
| `expand-both` | 217.5 | 223.3 | 39 ms | 36 ms |
| `k-hop` | 60.7 | 65.6 | 127 ms | 101 ms |
| `coexisting-shortest-path` | 206.6 | 216.8 | 45 ms | 40 ms |
| `earliest-arrival` | 92.7 | 96.7 | 76 ms | 66 ms |
| `latest-departure` | 86.9 | 91.8 | 94 ms | 78 ms |
| `fastest-duration` | 22.1 | 23.7 | 261 ms | 227 ms |

Typical result rates are approximately 36k-37k rows/s for `state-at`,
77k-86k rows/s for `events`/`history`, 390-403 rows/s for `expand-out`, and
650-670 rows/s for `expand-both`.

The following two rows are internal relational operator measurements, not
end-to-end public query API throughput:

| Internal operator | Cold qps | Warm qps | p99 |
|---|---:|---:|---:|
| `interval-join` | 477k | 476k | 6 us |
| `temporal-aggregate` | 457k | 452k | 7-8 us |

## Sustained Mixed Workload

The accepted sustained campaign used 32 writers, 32 readers, 1024 facts per
transaction, active maintenance, and ten query operations. Timed operation
elapsed time exceeded 1,800 seconds; every case exited successfully, reopened
successfully, and passed the hard gate.

| Operation | Facts/s |
|---|---:|
| `state-at` | 11,326 |
| `events` | 13,175 |
| `expand-out` | 13,980 |
| `temporal-aggregate` | 13,323 |
| `interval-join` | 10,029 |
| `k-hop` | 12,786 |
| `coexisting-shortest-path` | 14,253 |
| `earliest-arrival` | 12,272 |
| `latest-departure` | 17,498 |
| `fastest-duration` | 14,263 |

This sustained workload validates long-running stability, WAL/reopen behavior,
maintenance, and space accounting. Its latency is not directly comparable to
the cold/warm read matrix because 32 writers and 32 readers contend
simultaneously.

## Correctness and Resource Gates

- Debug CTest: `711/711` passed.
- QueryDifferential + QueryDelta under TSAN: `30/30` passed.
- QueryDifferential + QueryDelta under UBSAN: `30/30` passed.
- Projection `base`, `short-delta`, `long-delta`, and `partial-coverage` all
  passed typed canonical differential checks and reopen verification.
- Space audits passed with zero scratch bytes after reopen.
- `query_delta_bytes` controls both query memory admission and recovery repair
  memory; no separate hidden 512 MiB recovery allowance remains.

## Reproduction Commands

```bash
ctest --test-dir build/query-debug --output-on-failure
ctest --test-dir build/query-tsan -R 'QueryDifferential|QueryDelta' --output-on-failure
ctest --test-dir build/query-ubsan -R 'QueryDifferential|QueryDelta' --output-on-failure

benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release \
  --phase release-calibration \
  --duration-seconds 2 \
  --facts-per-txn 1,4,8,16,32,64,128,256,512,1024,2048 \
  --output /tmp/cedar-release-calibration
```

Raw evidence is retained under
`build/query-release/evidence/calibration-followup/`,
`write-idle-followup/`, `write-active-followup/`,
`read-cold-reduced/`, `read-warm-reduced/`, and
`mixed-sustained-final2/`.

